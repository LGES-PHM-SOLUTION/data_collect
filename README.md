# data_loader — 설비 센서 데이터 로더

MinIO 또는 로컬에 저장된 설비 센서 원본(parquet)을 내려받아,
**설비 1대 × 1초 단위의 와이드 테이블**(센서별 컬럼)로 변환해 주는 모듈입니다.

```python
from data_loader import load_sensor_data

df = load_sensor_data("raw/sensor", "EQP01", "2026-11-10", "2026-11-10 23:59:59")
```

---

## 0. 빠른 시작 (처음 받는 사용자)

준비물: **Git**, **Python 3.10 이상**, 사내 GitHub 계정과
**개인 액세스 토큰(PAT)** (GitHub → Settings → Developer settings → Personal access tokens),
MinIO 접속 정보(관리자에게 요청)

> `git clone` 중 `Password`를 물으면 GitHub 비밀번호가 아니라 **PAT**를 입력합니다.

### Linux / macOS (터미널)

```bash
# 1) 내려받기
git clone https://github.lgensol.com/jun-h-hong/data_processing.git
cd data_processing

# 2) 가상환경 + 패키지 설치
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 3) 접속 정보 설정 → 편집기로 열어 실제 값 입력
cp .env.example .env
vi .env

# 4) 동작 확인
python -c "from data_loader import load_sensor_data; print('OK')"
```

### Windows (cmd)

```bat
:: 1) 내려받기
git clone https://github.lgensol.com/jun-h-hong/data_processing.git
cd data_processing

:: 2) 가상환경 + 패키지 설치
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

:: 3) 접속 정보 설정 → 메모장에서 실제 값 입력 후 저장
copy .env.example .env
notepad .env

:: 4) 동작 확인
python -c "from data_loader import load_sensor_data; print('OK')"
```

> PowerShell에서는 활성화 명령이 `.venv\Scripts\Activate.ps1`이고, 복사는 `Copy-Item .env.example .env`입니다.
> GPU 가속(`requirements-gpu.txt`, cuDF)은 **Linux + NVIDIA GPU에서만** 동작합니다.

### 최신 코드로 업데이트

```bash
cd data_processing
git pull
```

`.env`는 Git이 관리하지 않으므로 `git pull`로 덮어써지지 않습니다.

### 다른 프로젝트에서 쓰기

`data_loader.py`를 작업 폴더에 복사하거나, 스크립트/노트북 상단에 경로를 추가합니다.

```python
import sys
sys.path.append("/path/to/data_processing")   # Windows: r"C:\path\to\data_processing"

from data_loader import load_sensor_data
```

> `.env`를 찾는 위치(그 폴더에 없으면 상위 폴더로 올라가며 찾음):
> - `.py` 스크립트로 실행 → **`data_loader.py`가 있는 폴더**
> - Jupyter 노트북 / `python -c` / 대화형 셸 → **현재 작업 폴더**
>
> 노트북을 다른 폴더에서 열 때는 그 위치에 `.env`를 두거나 `MINIO_*` 환경변수를 직접 설정하세요.

## 1. 설치

Python 3.10 이상이 필요합니다.

```bash
python -m venv .venv
source .venv/bin/activate                # Windows cmd: .venv\Scripts\activate
pip install -r requirements.txt          # pandas, pyarrow, minio, zstandard, python-dotenv
```

GPU 가속(cuDF, Linux 전용)을 쓰려면 `requirements.txt` 대신 아래를 설치합니다.

```bash
pip install -r requirements-gpu.txt      # requirements.txt + cudf-cu12
```

## 2. MinIO 접속 설정 (`.env`)

`.env.example`을 복사해 `.env`를 만들고 실제 값을 채웁니다.
`.env`는 `.gitignore`에 등록되어 있으므로 **절대 커밋되지 않습니다.**

```bash
cp .env.example .env                     # Windows cmd: copy .env.example .env
```

| 변수 | 필수 | 설명 |
|---|---|---|
| `MINIO_ENDPOINT` | ✅ | MinIO 주소 (예: `minio-host:9000`) |
| `MINIO_BUCKET` | ✅* | 버킷 이름. `minio://<bucket>/...` 형태로 경로에 버킷을 주면 생략 가능 |
| `MINIO_ACCESS_KEY` | ✅ | 액세스 키 |
| `MINIO_SECRET_KEY` | ✅ | 시크릿 키 |
| `MINIO_SECURE` | | `true`이면 HTTPS 사용 (기본 `false`) |
| `DATA_LOADER_USE_CUDF_PANDAS` | | `1`이면 cuDF pandas 가속 사용 (아래 6장 참고) |

> `.env`는 모듈을 import할 때 읽힙니다. 찾는 위치는 0장 "다른 프로젝트에서 쓰기"를 참고하세요.
> 로컬 파일만 읽을 때는 MinIO 설정이 없어도 됩니다.

## 3. 기본 사용법

### `load_sensor_data(data_path, eqp_id, start_time, end_time, columns=None)`

| 인자 | 타입 | 설명 |
|---|---|---|
| `data_path` | `str \| Path` | 원본 위치. 아래 세 가지 형식 지원 |
| `eqp_id` | `str` | 설비 ID (예: `"EQP01"`) |
| `start_time` | `str \| pd.Timestamp` | 조회 시작 시각 (포함) |
| `end_time` | `str \| pd.Timestamp` | 조회 종료 시각 (**포함**) |
| `columns` | `list[str] \| None` | 가져올 센서 이름 목록. `None`이면 전체 센서 |

**`data_path` 형식**

| 형식 | 예시 | 동작 |
|---|---|---|
| 로컬 파일/폴더 | `"/data/raw"`, `Path("a.parquet")` | 경로가 **존재하면** 로컬에서 읽음 (하위 폴더까지 재귀 탐색) |
| MinIO prefix | `"raw/sensor/2026"` | 로컬에 없는 경로면 `MINIO_BUCKET` 버킷의 prefix로 간주 |
| MinIO URL | `"minio://fade/raw/sensor"` | 지정한 버킷(`fade`)의 prefix에서 읽음 |

### 예제

```python
import pandas as pd

from data_loader import load_sensor_data

# 1) 로컬 폴더에서 하루치 전체 센서
df = load_sensor_data("/data/raw", "EQP01", "2026-11-10 00:00:00", "2026-11-10 23:59:59")

# 2) MinIO(.env의 기본 버킷)에서 특정 센서만
df = load_sensor_data(
    "raw/sensor",
    eqp_id="EQP01",
    start_time="2026-11-10 08:00",
    end_time="2026-11-10 12:00",
    columns=["TEMP", "PRESS"],
)

# 3) 버킷을 직접 지정
df = load_sensor_data("minio://fade/raw/sensor", "EQP01", "2026-11-09", "2026-11-10 23:59:59")

# 4) 센서값은 문자열로 반환되므로 숫자 변환 후 사용
sensor_cols = df.columns.drop(["eqp_id", "Sensor Time"])
df[sensor_cols] = df[sensor_cols].apply(pd.to_numeric, errors="coerce")
```

## 4. 입력 데이터 규칙

### 원본 parquet 스키마

다음 4개 컬럼이 반드시 있어야 합니다. 하나의 행에 여러 시각/값이 `|`로 이어져 있는 형태입니다.

| 컬럼 | 예시 | 설명 |
|---|---|---|
| `eqp_id` | `EQP01` | 설비 ID |
| `snsr_nm` | `TEMP` | 센서 이름 |
| `snsr_rd_tmst` | `2026-11-10 08:00:00.100\|2026-11-10 08:00:03.900` | 측정 시각 목록 |
| `snsr_data_val` | `25.1\|25.4` | 측정값 목록 (시각과 개수가 같아야 함) |

### 파일 선택 규칙

다운로드/읽기 대상 파일은 아래 조건을 **모두** 만족해야 합니다.

1. 확장자가 `.parquet` 또는 `.parquet.zst`
2. **파일명**에 `eqp_id` 문자열이 포함 (예: `EQP01_20261110.parquet`)
3. 경로에 `YYYYMMDD` 8자리 날짜가 있으면, 그 날짜가 `start_time ~ end_time`의 날짜 범위 안
   (날짜가 없으면 날짜 필터 없이 대상에 포함)

> 단일 파일 경로를 직접 지정한 경우에는 2번(파일명의 eqp_id) 검사를 하지 않습니다.

## 5. 출력 형식

```
  eqp_id         Sensor Time PRESS  TEMP
0  EQP01 2026-11-10 08:00:00   NaN  25.1
1  EQP01 2026-11-10 08:00:01  1.01   NaN
2  EQP01 2026-11-10 08:00:02  1.02   NaN
3  EQP01 2026-11-10 08:00:03   NaN  25.4
```

- 컬럼: `eqp_id`, `Sensor Time`, 이후 센서별 컬럼
  (`columns`를 지정하면 지정한 순서대로, 원본에 없는 센서는 전부 `NaN` 컬럼으로 생성)
- `Sensor Time`은 **초 단위로 내림**(`08:00:03.900` → `08:00:03`)
- 같은 초에 값이 여러 개면 **가장 마지막 값**을 사용 (중복 파일이 있어도 중복 행이 생기지 않음)
- 첫 시각~마지막 시각 사이를 **1초 간격으로 채우고**, 값이 없는 칸은 `NaN` (보간/ffill 안 함)
- 센서값 dtype은 **문자열(object)** — 필요하면 `pd.to_numeric`으로 변환
- 조건에 맞는 데이터가 없으면 빈 DataFrame 반환 (예외 아님)

### 원본 DataFrame만 변환하기

이미 메모리에 원본이 있으면 `convert_sensor_df`로 변환만 할 수 있습니다(기간 필터 없음).

```python
from data_loader import convert_sensor_df

wide = convert_sensor_df(raw_df, columns=["TEMP", "PRESS"])
```

## 6. GPU 가속 (선택)

cuDF의 pandas 가속을 쓰려면 **모듈 import 전에** 환경변수를 설정합니다.

```bash
DATA_LOADER_USE_CUDF_PANDAS=1 python my_script.py
```

또는 `.env`에 `DATA_LOADER_USE_CUDF_PANDAS=1`을 추가합니다.
`cudf`가 설치되어 있지 않으면 `ImportError`가 발생합니다.

## 7. 주의사항

- **메모리**: 기간 내 원본 전체를 메모리에 올린 뒤 변환합니다. 기간을 길게 잡거나 센서가 많으면
  메모리가 크게 늘어나므로, 필요한 센서만 `columns`로 지정하거나 기간을 나눠 호출하세요.
- **시각 형식**: `snsr_rd_tmst`는 한 번의 호출 안에서 형식이 통일되어 있어야 합니다.
  밀리초가 있는 값(`08:00:00.100`)과 없는 값(`08:00:01`)이 섞이면 시각 변환에서 `ValueError`가 납니다.
- **날짜 필터**: 경로 문자열에서 **처음 나오는** 8자리 숫자를 날짜로 해석합니다.
  상위 폴더명에 8자리 숫자가 있으면(예: `/backup_20250101/EQP01_20261110.parquet`)
  의도와 다르게 걸러질 수 있습니다.
- **eqp_id 부분 일치**: 파일명 검사는 부분 문자열 비교라 `EQP1`로 조회하면 `EQP10` 파일도
  내려받습니다(결과에서는 정확히 `EQP1`만 남음). 불필요한 다운로드가 생길 수 있습니다.
- MinIO 파일은 임시 폴더에 내려받아 읽은 뒤 **자동 삭제**됩니다(로컬에 남지 않음).
- 진행 상황은 `[함수명] ...` 형식으로 표준출력에 표시됩니다.

## 8. 주요 오류

| 오류 | 원인 |
|---|---|
| `ValueError: eqp_id는 비어 있을 수 없습니다.` | `eqp_id`가 빈 문자열 |
| `ValueError: start_time은 end_time보다 늦을 수 없습니다.` | 기간 역전 |
| `TypeError: columns는 센서 이름 문자열의 리스트이거나 None이어야 합니다.` | `columns="TEMP"`처럼 문자열 하나를 전달 → `["TEMP"]`로 수정 |
| `ValueError: MinIO 설정 환경변수가 필요합니다: ...` | `.env` 누락/실행 위치에 `.env`가 없음, 또는 **로컬 경로 오타**(존재하지 않는 로컬 경로는 MinIO prefix로 처리됨) |
| `ValueError: 필수 컬럼이 없습니다: [...]` | 원본 parquet 스키마가 다름 |
| `ValueError: snsr_rd_tmst와 snsr_data_val의 \| 구분 항목 수가 서로 다릅니다.` | 원본 행의 시각/값 개수 불일치 |
