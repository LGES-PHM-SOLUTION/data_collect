

# 실행시킬 경우, .env파일이 해당 경로에 존재해야 하며, MinIO 관련 환경변수가 설정되어 있어야 합니다.
from __future__ import annotations

import os
import re
import shutil
import tempfile
from datetime import date
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv

load_dotenv()

if os.getenv("DATA_LOADER_USE_CUDF_PANDAS", "").strip().lower() in {
    "1",
    "true",
    "yes",
}:
    try:
        import cudf.pandas
    except ImportError as exc:
        raise ImportError(
            "GPU 가속을 사용하려면 requirements-gpu.txt로 cudf-cu12를 설치하세요."
        ) from exc

    cudf.pandas.install()

import pandas as pd


_REQUIRED_COLUMNS = {"eqp_id", "snsr_nm", "snsr_rd_tmst", "snsr_data_val"}
_RAW_COLUMNS = ["eqp_id", "snsr_nm", "snsr_rd_tmst", "snsr_data_val"]
_PARQUET_SUFFIXES = (".parquet", ".parquet.zst")

def _empty_sensor_df(columns: list[str] | None) -> pd.DataFrame:
    sensor_columns = columns or []
    return pd.DataFrame(columns=["eqp_id", "Sensor Time", *sensor_columns])


def convert_sensor_df(
    df: pd.DataFrame, columns: list[str] | None = None
) -> pd.DataFrame:
    """Convert pipe-delimited raw sensor rows to one row per equipment-second."""
    print(f"[convert_sensor_df] 시작: 입력 {len(df):,}행", flush=True)
    missing = _REQUIRED_COLUMNS.difference(df.columns)
    if missing:
        raise ValueError(f"필수 컬럼이 없습니다: {sorted(missing)}")
    if columns is not None and (
        isinstance(columns, str) or not all(isinstance(name, str) for name in columns)
    ):
        raise TypeError("columns는 센서 이름 문자열의 리스트이거나 None이어야 합니다.")

    selected_columns = list(dict.fromkeys(columns)) if columns is not None else None

    data = df[_RAW_COLUMNS].copy()
    if selected_columns is not None:
        data = data[data["snsr_nm"].isin(selected_columns)].copy()
        print(
            f"[convert_sensor_df] 센서 필터 완료: {len(data):,}행, "
            f"{len(selected_columns)}개 센서",
            flush=True,
        )
        if data.empty:
            print("[convert_sensor_df] 종료: 선택 센서 데이터 없음", flush=True)
            return _empty_sensor_df(selected_columns)

    print("[convert_sensor_df] 시각·값 문자열 분리 시작", flush=True)
    for column in ("snsr_rd_tmst", "snsr_data_val"):
        data[column] = data[column].fillna("").astype(str).str.split("|")
    print("[convert_sensor_df] 시각·값 문자열 분리 완료", flush=True)

    lengths_match = data["snsr_rd_tmst"].str.len() == data["snsr_data_val"].str.len()
    if not lengths_match.all():
        raise ValueError("snsr_rd_tmst와 snsr_data_val의 | 구분 항목 수가 서로 다릅니다.")

    print("[convert_sensor_df] explode 시작", flush=True)
    long = data.explode(["snsr_rd_tmst", "snsr_data_val"], ignore_index=True)
    print(f"[convert_sensor_df] explode 완료: {len(long):,}행", flush=True)
    long["snsr_rd_tmst"] = long["snsr_rd_tmst"].str.strip()
    long = long[long["snsr_rd_tmst"].ne("")].copy()
    print("[convert_sensor_df] 시각 변환·정렬 시작", flush=True)
    long["Sensor Time"] = pd.to_datetime(long["snsr_rd_tmst"], errors="raise")
    long = long.sort_values("Sensor Time")
    long["Sensor Time"] = long["Sensor Time"].dt.floor("s")

    print(
        f"[convert_sensor_df] pivot 시작: {len(long):,}행, "
        f"{long['snsr_nm'].nunique():,}개 센서",
        flush=True,
    )
    converted = long.pivot_table(
        index=["eqp_id", "Sensor Time"],
        columns="snsr_nm",
        values="snsr_data_val",
        aggfunc="last",
        sort=True,
    )
    print(
        f"[convert_sensor_df] pivot 완료: {converted.shape[0]:,}행 x "
        f"{converted.shape[1]:,}열",
        flush=True,
    )
    if converted.empty:
        print("[convert_sensor_df] 종료: 변환 결과 없음", flush=True)
        return _empty_sensor_df(selected_columns)

    if selected_columns is not None:
        converted = converted.reindex(columns=selected_columns)

    resampled = []
    for equipment_id, equipment_data in converted.groupby(level="eqp_id", sort=False):
        equipment_data = equipment_data.droplevel("eqp_id")
        print(
            f"[convert_sensor_df] 초 단위 재색인 시작: {equipment_id}, "
            f"{len(equipment_data):,}개 시각",
            flush=True,
        )
        second_index = pd.date_range(
            equipment_data.index.min(),
            equipment_data.index.max(),
            freq="s",
            name="Sensor Time",
        )
        equipment_data = equipment_data.reindex(second_index)
        equipment_data.insert(0, "eqp_id", equipment_id)
        resampled.append(equipment_data.reset_index())

    print("[convert_sensor_df] 최종 결합 시작", flush=True)
    converted = pd.concat(resampled, ignore_index=True).sort_values(
        ["Sensor Time", "eqp_id"], ignore_index=True
    )
    sensor_columns = [
        column for column in converted.columns if column not in {"eqp_id", "Sensor Time"}
    ]
    if selected_columns is not None:
        sensor_columns = selected_columns
    converted = converted[["eqp_id", "Sensor Time", *sensor_columns]]
    converted.columns.name = None
    print(f"[convert_sensor_df] 완료: {len(converted):,}행", flush=True)
    return converted


def _is_parquet_path(path: Path) -> bool:
    return str(path).lower().endswith(_PARQUET_SUFFIXES)


def _is_in_requested_dates(path: str, start_date: date, end_date: date) -> bool:
    match = re.search(r"(?<!\d)(\d{8})(?!\d)", path)
    if match is None:
        return True
    try:
        object_date = date(int(match[1][:4]), int(match[1][4:6]), int(match[1][6:8]))
    except ValueError:
        return True
    return start_date <= object_date <= end_date


def _candidate_local_files(
    data_path: Path, eqp_id: str, start_date: date, end_date: date
) -> list[Path]:
    if data_path.is_file():
        if _is_parquet_path(data_path) and _is_in_requested_dates(
            str(data_path), start_date, end_date
        ):
            return [data_path]
        return []
    elif data_path.is_dir():
        candidates = list(data_path.rglob("*"))
    else:
        raise FileNotFoundError(f"데이터 경로를 찾을 수 없습니다: {data_path}")

    return [
        path
        for path in candidates
        if path.is_file()
        and _is_parquet_path(path)
        and eqp_id in path.name
        and _is_in_requested_dates(str(path), start_date, end_date)
    ]


def _read_parquet(path: Path) -> pd.DataFrame:
    if not str(path).lower().endswith(".zst"):
        return pd.read_parquet(path, columns=_RAW_COLUMNS)

    try:
        import zstandard
    except ImportError as exc:
        raise ImportError(
            "'.parquet.zst' 파일을 읽으려면 zstandard 패키지가 필요합니다."
        ) from exc

    with tempfile.NamedTemporaryFile(suffix=".parquet") as decompressed:
        with path.open("rb") as compressed:
            zstandard.ZstdDecompressor().copy_stream(compressed, decompressed)
        decompressed.flush()
        return pd.read_parquet(decompressed.name, columns=_RAW_COLUMNS)


def _minio_settings(bucket_from_url: str | None) -> tuple[str, str, str, bool, str]:
    endpoint = os.getenv("MINIO_ENDPOINT")
    bucket = bucket_from_url or os.getenv("MINIO_BUCKET")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    missing = [
        name
        for name, value in (
            ("MINIO_ENDPOINT", endpoint),
            ("MINIO_BUCKET", bucket),
            ("MINIO_ACCESS_KEY", access_key),
            ("MINIO_SECRET_KEY", secret_key),
        )
        if not value
    ]
    if missing:
        raise ValueError(f"MinIO 설정 환경변수가 필요합니다: {', '.join(missing)}")

    secure = os.getenv("MINIO_SECURE", "false").strip().lower() in {"1", "true", "yes"}
    return endpoint, bucket, access_key, secure, secret_key


def _read_minio_prefix(
    prefix: str,
    eqp_id: str,
    start_date: date,
    end_date: date,
    bucket_from_url: str | None = None,
) -> pd.DataFrame:
    try:
        from minio import Minio
    except ImportError as exc:
        raise ImportError("MinIO에서 읽으려면 minio 패키지가 필요합니다.") from exc

    endpoint, bucket, access_key, secure, secret_key = _minio_settings(bucket_from_url)
    client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
    print(f"[_read_minio_prefix] 오브젝트 목록 조회 시작: {prefix}, eqp={eqp_id}", flush=True)
    objects = client.list_objects(bucket, prefix=prefix.strip("/"), recursive=True)
    object_names = [
        obj.object_name
        for obj in objects
        if eqp_id in Path(obj.object_name).name
        and _is_parquet_path(Path(obj.object_name))
        and _is_in_requested_dates(obj.object_name, start_date, end_date)
    ]
    print(f"[_read_minio_prefix] 대상 오브젝트 검색 완료: {len(object_names)}개", flush=True)
    if not object_names:
        return pd.DataFrame(columns=["eqp_id", "Sensor Time"])

    frames = []
    with tempfile.TemporaryDirectory() as temporary_directory:
        for index, object_name in enumerate(object_names, start=1):
            print(
                f"[_read_minio_prefix] {index}/{len(object_names)} 다운로드 시작: "
                f"{Path(object_name).name}",
                flush=True,
            )
            raw_path = Path(temporary_directory) / Path(object_name).name
            response = client.get_object(bucket, object_name)
            try:
                with raw_path.open("wb") as downloaded:
                    shutil.copyfileobj(response, downloaded)
            finally:
                response.close()
                response.release_conn()
            print(f"[_read_minio_prefix] {index}/{len(object_names)} Parquet 읽기 시작", flush=True)
            frames.append(_read_parquet(raw_path))
            print(
                f"[_read_minio_prefix] {index}/{len(object_names)} Parquet 읽기 완료: "
                f"{len(frames[-1]):,}행",
                flush=True,
            )
    result = pd.concat(frames, ignore_index=True)
    print(f"[_read_minio_prefix] 전체 원본 결합 완료: {len(result):,}행", flush=True)
    return result


def load_sensor_data(
    data_path: str | Path,
    eqp_id: str,
    start_time: str | pd.Timestamp,
    end_time: str | pd.Timestamp,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    """Load, convert, and time-filter raw sensor parquet data.

    ``data_path`` may be a local file/directory, a MinIO prefix, or
    ``minio://<bucket>/<prefix>``. ``columns`` selects sensor names; ``None``
    returns all sensors. MinIO access uses MINIO_ENDPOINT,
    MINIO_ACCESS_KEY, MINIO_SECRET_KEY, and MINIO_BUCKET (unless the URL
    specifies a bucket); MINIO_SECURE optionally enables HTTPS. To enable
    cuDF's pandas accelerator, set DATA_LOADER_USE_CUDF_PANDAS=1 before
    importing this module.
    """
    if not str(eqp_id).strip():
        raise ValueError("eqp_id는 비어 있을 수 없습니다.")
    if columns is not None and (
        isinstance(columns, str) or not all(isinstance(name, str) for name in columns)
    ):
        raise TypeError("columns는 센서 이름 문자열의 리스트이거나 None이어야 합니다.")
    selected_columns = list(dict.fromkeys(columns)) if columns is not None else None

    start = pd.Timestamp(start_time)
    end = pd.Timestamp(end_time)
    if start > end:
        raise ValueError("start_time은 end_time보다 늦을 수 없습니다.")

    path_text = str(data_path)
    parsed = urlparse(path_text)
    print(
        f"[load_sensor_data] 시작: eqp={eqp_id}, {start} ~ {end}, "
        f"센서={len(selected_columns) if selected_columns is not None else '전체'}",
        flush=True,
    )
    if parsed.scheme == "minio":
        print("[load_sensor_data] MinIO 원본 조회 시작", flush=True)
        raw_df = _read_minio_prefix(
            parsed.path.lstrip("/"),
            str(eqp_id),
            start.date(),
            end.date(),
            parsed.netloc or None,
        )
    else:
        local_path = Path(data_path).expanduser()
        if local_path.exists():
            print(f"[load_sensor_data] 로컬 파일 탐색 시작: {local_path}", flush=True)
            raw_files = _candidate_local_files(
                local_path, str(eqp_id), start.date(), end.date()
            )
            print(f"[load_sensor_data] 로컬 파일 탐색 완료: {len(raw_files)}개", flush=True)
            if not raw_files:
                return _empty_sensor_df(selected_columns)
            raw_frames = []
            for index, path in enumerate(raw_files, start=1):
                print(
                    f"[load_sensor_data] {index}/{len(raw_files)} Parquet 읽기 시작: "
                    f"{path.name}",
                    flush=True,
                )
                raw_frames.append(_read_parquet(path))
                print(
                    f"[load_sensor_data] {index}/{len(raw_files)} Parquet 읽기 완료: "
                    f"{len(raw_frames[-1]):,}행",
                    flush=True,
                )
            print("[load_sensor_data] 로컬 원본 결합 시작", flush=True)
            raw_df = pd.concat(raw_frames, ignore_index=True)
            print(f"[load_sensor_data] 로컬 원본 결합 완료: {len(raw_df):,}행", flush=True)
        else:
            print("[load_sensor_data] MinIO 원본 조회 시작", flush=True)
            raw_df = _read_minio_prefix(
                path_text, str(eqp_id), start.date(), end.date()
            )

    print(f"[load_sensor_data] 원본 조회 완료: {len(raw_df):,}행", flush=True)
    if raw_df.empty:
        return _empty_sensor_df(selected_columns)

    missing = _REQUIRED_COLUMNS.difference(raw_df.columns)
    if missing:
        raise ValueError(f"필수 컬럼이 없습니다: {sorted(missing)}")
    raw_df = raw_df[raw_df["eqp_id"].astype(str).eq(str(eqp_id))]
    if selected_columns is not None:
        raw_df = raw_df[raw_df["snsr_nm"].isin(selected_columns)]
        print(f"[load_sensor_data] 센서 필터 완료: {len(raw_df):,}행", flush=True)
    if raw_df.empty:
        return _empty_sensor_df(selected_columns)

    print("[load_sensor_data] convert_sensor_df 시작", flush=True)
    converted = convert_sensor_df(raw_df, columns=selected_columns)
    print(f"[load_sensor_data] convert_sensor_df 완료: {len(converted):,}행", flush=True)
    in_period = converted["Sensor Time"].between(start, end, inclusive="both")
    result = converted.loc[in_period].reset_index(drop=True)
    print(f"[load_sensor_data] 기간 필터 완료: {len(result):,}행", flush=True)
    return result
