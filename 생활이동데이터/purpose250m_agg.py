"""
PURPOSE 250M 데이터 정제 및 집계 스크립트
- TOTAL_CNT 컬럼 직접 사용 (원본에 포함된 값)
- IN_FORN_DIV_NM 필터/컬럼 제거 (내국인 전용 파일 기준)
- 시간대 범주화: 05-08 / 08-18 / 18-22 / 22-05
- 출발(O_CELL) / 도착(D_CELL) 기준 각각 집계
- ※ ETL_YMD 미포함 → 파일 내 전체 기간 합계 (월 합계)
"""

import pandas as pd
import os

# ── 경로 설정 (필요 시 수정) ────────────────────────────────
INPUT_FILE = "PURPOSE_250M_202403.csv"   # ← 원본 파일 경로로 변경
OUTPUT_DIR = "./"                        # ← 결과 저장 폴더로 변경
# ────────────────────────────────────────────────────────────

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ── 시간대 범주화 함수 ──────────────────────────────────────
def time_band(hour: pd.Series) -> pd.Series:
    result = pd.Series("22-05", index=hour.index)
    result[(hour >= 5)  & (hour < 8)]  = "05-08"
    result[(hour >= 8)  & (hour < 18)] = "08-18"
    result[(hour >= 18) & (hour < 22)] = "18-22"
    return result


# ── 데이터 로드 ─────────────────────────────────────────────
print("▶ 데이터 로드 중...")
df = pd.read_csv(INPUT_FILE, encoding="cp949")
print(f"  원본: {df.shape[0]:,}행 × {df.shape[1]}열")

# 원본 TOTAL_CNT 컬럼 직접 사용
df = df.rename(columns={"TOTAL_CNT": "total_pop"})

# 시간대 범주 순서
BAND_ORDER = ["05-08", "08-18", "18-22", "22-05"]


# ════════════════════════════════════════════════════════════
# 1. 출발 기준 집계 (O_CELL + ST_TIME_CD)
# ════════════════════════════════════════════════════════════
print("\n▶ 출발 기준 집계 중...")

df["time_band"] = time_band(df["ST_TIME_CD"])

depart = (
    df.groupby(["O_CELL_ID", "O_CELL_X", "O_CELL_Y", "time_band"], observed=True)
    ["total_pop"]
    .sum()
    .reset_index()
)
depart["time_band"] = pd.Categorical(depart["time_band"], categories=BAND_ORDER, ordered=True)
depart = depart.sort_values(["O_CELL_ID", "time_band"]).reset_index(drop=True)

out_depart = os.path.join(OUTPUT_DIR, "PURPOSE250M_departure_agg.csv")
depart.to_csv(out_depart, index=False, encoding="utf-8-sig")
print(f"  저장 완료: {out_depart}  ({depart.shape[0]:,}행 × {depart.shape[1]}열)")


# ════════════════════════════════════════════════════════════
# 2. 도착 기준 집계 (D_CELL + FNS_TIME_CD)
# ════════════════════════════════════════════════════════════
print("\n▶ 도착 기준 집계 중...")

df["time_band"] = time_band(df["FNS_TIME_CD"])

arrive = (
    df.groupby(["D_CELL_ID", "D_CELL_X", "D_CELL_Y", "time_band"], observed=True)
    ["total_pop"]
    .sum()
    .reset_index()
    .rename(columns={
        "D_CELL_ID": "O_CELL_ID",   # 컬럼명 통일 (출발과 동일한 구조)
        "D_CELL_X":  "O_CELL_X",
        "D_CELL_Y":  "O_CELL_Y",
    })
)
arrive["time_band"] = pd.Categorical(arrive["time_band"], categories=BAND_ORDER, ordered=True)
arrive = arrive.sort_values(["O_CELL_ID", "time_band"]).reset_index(drop=True)

out_arrive = os.path.join(OUTPUT_DIR, "PURPOSE250M_arrival_agg.csv")
arrive.to_csv(out_arrive, index=False, encoding="utf-8-sig")
print(f"  저장 완료: {out_arrive}  ({arrive.shape[0]:,}행 × {arrive.shape[1]}열)")


# ════════════════════════════════════════════════════════════
# 3. 검증 출력
# ════════════════════════════════════════════════════════════
print("\n── 출발 집계 샘플 ──")
print(depart.head(8).to_string(index=False))
print("\n── 도착 집계 샘플 ──")
print(arrive.head(8).to_string(index=False))
print("\n✅ 완료")
