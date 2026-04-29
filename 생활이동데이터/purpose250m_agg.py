"""
PURPOSE 250M 데이터 정제 및 집계 스크립트
- 일자별 파일 순회 (20250401 ~ 20250430)
- TOTAL_CNT 컬럼 직접 사용
- chunksize로 메모리 절약
- OD 쌍(O_CELL + D_CELL) 유지한 채 시간대 범주화 후 합산
- 시간대 범주화: 05-08 / 08-18 / 18-22 / 22-05
- 일자별로 departure / arrival 파일 각각 저장
- 출력 컬럼명 전체 소문자
"""

import pandas as pd
import os
from datetime import datetime, timedelta

# ── 경로 설정 (필요 시 수정) ────────────────────────────────
INPUT_DIR  = r"E:\202504_bo78\202504"
OUTPUT_DIR = r"E:\202504_bo78\output"
START_DATE = "20250401"
END_DATE   = "20250430"
CHUNKSIZE  = 500000   # 메모리 부족 시 줄이기 (예: 200000)
# ────────────────────────────────────────────────────────────

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ── 시간대 범주화 함수 ──────────────────────────────────────
def time_band(hour: pd.Series) -> pd.Series:
    result = pd.Series("22-05", index=hour.index)
    result[(hour >= 5)  & (hour < 8)]  = "05-08"
    result[(hour >= 8)  & (hour < 18)] = "08-18"
    result[(hour >= 18) & (hour < 22)] = "18-22"
    return result

BAND_ORDER = ["05-08", "08-18", "18-22", "22-05"]
OD_KEYS = [
    "o_cell_id", "o_cell_x", "o_cell_y",
    "d_cell_id", "d_cell_x", "d_cell_y",
]

# ── 날짜 목록 생성 ──────────────────────────────────────────
start = datetime.strptime(START_DATE, "%Y%m%d")
end   = datetime.strptime(END_DATE,   "%Y%m%d")
date_list = []
cur = start
while cur <= end:
    date_list.append(cur.strftime("%Y%m%d"))
    cur += timedelta(days=1)

print(f"▶ 처리 대상: {date_list[0]} ~ {date_list[-1]}  ({len(date_list)}일)")


# ── 청크별 집계 함수 ────────────────────────────────────────
def aggregate_chunks(file_path, time_col):
    chunk_results = []
    for chunk in pd.read_csv(file_path, encoding="cp949", chunksize=CHUNKSIZE):
        chunk.columns = chunk.columns.str.lower()          # 컬럼명 소문자 변환
        chunk = chunk.rename(columns={"total_cnt": "total_pop"})
        chunk["time_band"] = time_band(chunk[time_col])
        agg = (
            chunk.groupby(OD_KEYS + ["time_band"], observed=True)
            ["total_pop"].sum().reset_index()
        )
        chunk_results.append(agg)

    result = (
        pd.concat(chunk_results, ignore_index=True)
        .groupby(OD_KEYS + ["time_band"], observed=True)
        ["total_pop"].sum().reset_index()
    )
    result["time_band"] = pd.Categorical(result["time_band"], categories=BAND_ORDER, ordered=True)
    result = result.sort_values(["o_cell_id", "d_cell_id", "time_band"]).reset_index(drop=True)
    return result


# ════════════════════════════════════════════════════════════
# 날짜별 순회 → 일자별 파일 저장
# ════════════════════════════════════════════════════════════
for date_str in date_list:
    file_name = f"PURPOSE_in_250M_{date_str}.csv"
    file_path = os.path.join(INPUT_DIR, file_name)

    if not os.path.exists(file_path):
        print(f"  ⚠️  파일 없음, 건너뜀: {file_name}")
        continue

    print(f"  처리 중: {file_name}", end="  ")

    depart = aggregate_chunks(file_path, time_col="st_time_cd")
    out_depart = os.path.join(OUTPUT_DIR, f"PURPOSE250M_departure_{date_str}.csv")
    depart.to_csv(out_depart, index=False, encoding="utf-8-sig")

    arrive = aggregate_chunks(file_path, time_col="fns_time_cd")
    out_arrive = os.path.join(OUTPUT_DIR, f"PURPOSE250M_arrival_{date_str}.csv")
    arrive.to_csv(out_arrive, index=False, encoding="utf-8-sig")

    print(f"→ departure({depart.shape[0]:,}행) / arrival({arrive.shape[0]:,}행) 저장 완료")

print("\n✅ 완료")
