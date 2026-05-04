import pandas as pd
import geopandas as gpd
import glob, os

# ── 경로 설정 ────────────────────────────────────────────
MOVEMENT_DIR = r"E:\202504_bo78\output"
SHP_FILE     = r"match.shp"
OUTPUT_FILE  = r"E:\202504_bo78\격자별_일평균_이동량.csv"
# ────────────────────────────────────────────────────────

# Step 1. 서울 격자 ID 목록
print("▶ 서울 격자 로드 중...")
gdf = gpd.read_file(SHP_FILE)
seoul_cells = set(gdf['CELL_ID'].astype(str).tolist())
print(f"  서울 격자 수: {len(seoul_cells):,}개")

# Step 2. departure 파일 순회
files = sorted(glob.glob(os.path.join(MOVEMENT_DIR, "PURPOSE250M_departure_*.csv")))
day_count = len(files)
print(f"\n▶ departure 파일 {day_count}개 처리 중... (일 평균 기준: {day_count}일)")

outflow_list = []
inflow_list  = []

for f in files:
    fname = os.path.basename(f)
    for chunk in pd.read_csv(f, chunksize=500000, low_memory=False):
        # 숫자 ID 제거
        chunk = chunk[~chunk['o_cell_id'].astype(str).str.match(r'^\d+$')]
        chunk = chunk[~chunk['d_cell_id'].astype(str).str.match(r'^\d+$')]

        # 서울 격자 필터
        chunk = chunk[
            chunk['o_cell_id'].astype(str).isin(seoul_cells) &
            chunk['d_cell_id'].astype(str).isin(seoul_cells)
        ]
        if len(chunk) == 0:
            continue

        outflow_list.append(chunk.groupby('o_cell_id')['total_pop'].sum().reset_index())
        inflow_list.append(chunk.groupby('d_cell_id')['total_pop'].sum().reset_index())

    print(f"  {fname} 완료")

# Step 3. 전체 합산
print("\n▶ 전체 합산 및 일 평균 계산 중...")
outflow = (
    pd.concat(outflow_list, ignore_index=True)
    .groupby('o_cell_id')['total_pop'].sum().reset_index()
    .rename(columns={'o_cell_id': 'CELL_ID', 'total_pop': 'outflow'})
)
inflow = (
    pd.concat(inflow_list, ignore_index=True)
    .groupby('d_cell_id')['total_pop'].sum().reset_index()
    .rename(columns={'d_cell_id': 'CELL_ID', 'total_pop': 'inflow'})
)

# Step 4. 병합 + 일 평균
result = outflow.merge(inflow, on='CELL_ID', how='outer').fillna(0)
result['outflow'] = (result['outflow'] / day_count).round(2)
result['inflow']  = (result['inflow']  / day_count).round(2)
result['total']   = (result['outflow'] + result['inflow']).round(2)
result = result.sort_values('total', ascending=False).reset_index(drop=True)

result.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
print(f"\n✅ 저장 완료: {OUTPUT_FILE}")
print(f"   격자 수: {len(result):,}개  /  기준 일수: {day_count}일")
print(f"\n상위 10개:")
print(result.head(10).to_string(index=False))