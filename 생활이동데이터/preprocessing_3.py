"""
생활이동 데이터 전처리 스크립트

[최종 결과물: 4개 파일]
1. PURPOSE250M_departure_filtered.csv : 격자별 집계 - departure (MCLP용)
2. PURPOSE250M_arrival_filtered.csv   : 격자별 집계 - arrival (MCLP용)
3. final_departure.csv                : 역별 집계 - departure (분석용)
4. final_arrival.csv                  : 역별 집계 - arrival (분석용)

[필터링 조건: 양방향]
- 역 800m 내 → 공원/러너코스
- 공원/러너코스 → 역 800m 내

[Voronoi 배정]
- 2개 이상 역 버퍼에 겹치는 격자 → 가장 가까운 역으로 배정 (파일 3,4에만 적용)

[시간대 재분류]
- 05-08 / 18-22 / 기타 (08-18 + 22-05)
"""

import pandas as pd
import geopandas as gpd
import glob, os
from shapely.geometry import LineString
import polyline as pl

# ── 경로 설정 (필요 시 수정) ────────────────────────────────
SUBWAY_FILE   = r"C:\Users\JS Kim\Documents\카카오톡 받은 파일\station_cluster_result_log_selected_scaled_weighted_800m.csv"
PARK_FILE     = r"C:\Users\JS Kim\Downloads\공원_경계_수정.geojson"
RUNNER_FILE   = r"C:\Users\JS Kim\Documents\카카오톡 받은 파일\strava_50percent_중복제거.csv"
MOVEMENT_DIR  = r"D:\output"     # departure/arrival CSV 폴더
OUTPUT_DIR    = r"D:\output\filtered"
BUFFER_M      = 800
# ────────────────────────────────────────────────────────────

os.makedirs(OUTPUT_DIR, exist_ok=True)
CRS_UTMK = "EPSG:5179"
CRS_WGS  = "EPSG:4326"


# ── 시간대 재분류 함수 ──────────────────────────────────────
def reclassify_time_band(band: pd.Series) -> pd.Series:
    result = pd.Series("기타", index=band.index)
    result[band == "05-08"] = "05-08"
    result[band == "18-22"] = "18-22"
    return result


# ════════════════════════════════════════════════════════════
# Step 1. 전체 격자 포인트 GeoDataFrame 생성
# ════════════════════════════════════════════════════════════
print("▶ Step1. 전체 격자 포인트 생성 중...")

sample_file = sorted(glob.glob(os.path.join(MOVEMENT_DIR, "PURPOSE250M_departure_*.csv")))[0]
df_sample = pd.read_csv(sample_file, low_memory=False)
df_sample = df_sample[~df_sample['o_cell_id'].astype(str).str.match(r'^\d+$')]
df_sample = df_sample[~df_sample['d_cell_id'].astype(str).str.match(r'^\d+$')]

o_cells = df_sample[['o_cell_id','o_cell_x','o_cell_y']].drop_duplicates()
o_gdf = gpd.GeoDataFrame(
    o_cells,
    geometry=gpd.points_from_xy(o_cells['o_cell_x'], o_cells['o_cell_y']),
    crs=CRS_UTMK
)
d_cells = df_sample[['d_cell_id','d_cell_x','d_cell_y']].drop_duplicates()
d_gdf = gpd.GeoDataFrame(
    d_cells,
    geometry=gpd.points_from_xy(d_cells['d_cell_x'], d_cells['d_cell_y']),
    crs=CRS_UTMK
)
print(f"  O_CELL: {len(o_gdf):,}개  /  D_CELL: {len(d_gdf):,}개")


# ════════════════════════════════════════════════════════════
# Step 2. 지하철역 800m 버퍼 격자 추출 + Voronoi 배정
# ════════════════════════════════════════════════════════════
print("\n▶ Step2. 지하철역 버퍼 격자 추출 + Voronoi 배정 중...")

subway = pd.read_csv(SUBWAY_FILE)
subway_gdf = gpd.GeoDataFrame(
    subway,
    geometry=gpd.points_from_xy(subway['경도'], subway['위도']),
    crs=CRS_WGS
).to_crs(CRS_UTMK)

subway_buffer = subway_gdf.copy()
subway_buffer['geometry'] = subway_buffer.geometry.buffer(BUFFER_M)

# 버퍼 내 O_CELL 추출 (역 정보 포함)
o_in_subway = gpd.sjoin(
    o_gdf,
    subway_buffer[['역사명','호선','geometry']],
    how='inner', predicate='within'
)[['o_cell_id','o_cell_x','o_cell_y','역사명','호선']]

# 겹치는 격자 확인 (2개 이상 역에 속하는 격자)
overlap_cells = o_in_subway[o_in_subway.duplicated('o_cell_id', keep=False)]
non_overlap_cells = o_in_subway[~o_in_subway.duplicated('o_cell_id', keep=False)]

print(f"  겹치는 격자 수: {overlap_cells['o_cell_id'].nunique():,}개")
print(f"  안 겹치는 격자 수: {len(non_overlap_cells):,}개")

# Voronoi: 겹치는 격자 → 가장 가까운 역으로 배정
overlap_gdf = gpd.GeoDataFrame(
    overlap_cells[['o_cell_id','o_cell_x','o_cell_y']].drop_duplicates(),
    geometry=gpd.points_from_xy(
        overlap_cells.drop_duplicates('o_cell_id')['o_cell_x'],
        overlap_cells.drop_duplicates('o_cell_id')['o_cell_y']
    ),
    crs=CRS_UTMK
)
voronoi_assigned = gpd.sjoin_nearest(
    overlap_gdf,
    subway_gdf[['역사명','호선','geometry']],
    how='left'
)[['o_cell_id','o_cell_x','o_cell_y','역사명','호선']]

# 겹치지 않는 격자 + Voronoi 배정된 격자 합치기
o_cell_station_map = pd.concat([
    non_overlap_cells[['o_cell_id','o_cell_x','o_cell_y','역사명','호선']],
    voronoi_assigned
], ignore_index=True).drop_duplicates('o_cell_id')

subway_o_cells = set(o_cell_station_map['o_cell_id'])

# D_CELL 기준 역 격자
d_subway = gpd.sjoin(
    d_gdf, subway_buffer[['역사명','호선','geometry']],
    how='inner', predicate='within'
)[['d_cell_id']].drop_duplicates()
subway_d_cells = set(d_subway['d_cell_id'])

print(f"  최종 역 O_CELL: {len(subway_o_cells):,}개")


# ════════════════════════════════════════════════════════════
# Step 3. 공원 GeoJSON 격자 추출
# ════════════════════════════════════════════════════════════
print("\n▶ Step3. 공원 격자 추출 중...")

park_gdf = gpd.read_file(PARK_FILE).to_crs(CRS_UTMK)

o_park = gpd.sjoin(o_gdf, park_gdf[['공원명','공원종류','geometry']], how='inner', predicate='within')[['o_cell_id']].drop_duplicates()
d_park = gpd.sjoin(d_gdf, park_gdf[['공원명','공원종류','geometry']], how='inner', predicate='within')[['d_cell_id','공원명','공원종류']].drop_duplicates('d_cell_id')

park_o_cells = set(o_park['o_cell_id'])
park_d_cells = set(d_park['d_cell_id'])
print(f"  공원 O_CELL: {len(park_o_cells):,}개  /  D_CELL: {len(park_d_cells):,}개")


# ════════════════════════════════════════════════════════════
# Step 4. 러너코스 polyline 격자 추출
# ════════════════════════════════════════════════════════════
print("\n▶ Step4. 러너코스 격자 추출 중...")

runner = pd.read_csv(RUNNER_FILE)

def decode_to_linestring(encoded):
    try:
        coords = pl.decode(encoded)
        if len(coords) < 2:
            return None
        return LineString([(lon, lat) for lat, lon in coords])
    except:
        return None

runner_gdf = gpd.GeoDataFrame(
    runner,
    geometry=runner['polyline'].apply(decode_to_linestring),
    crs=CRS_WGS
).dropna(subset=['geometry']).to_crs(CRS_UTMK)
runner_gdf['geometry'] = runner_gdf.geometry.buffer(50)

o_runner = gpd.sjoin(o_gdf, runner_gdf[['segment_id','name','geometry']], how='inner', predicate='within')[['o_cell_id']].drop_duplicates()
d_runner = gpd.sjoin(d_gdf, runner_gdf[['segment_id','name','geometry']], how='inner', predicate='within')[['d_cell_id','name']].drop_duplicates('d_cell_id')
d_runner = d_runner.rename(columns={'name': '공원명'})
d_runner['공원종류'] = '러너코스'

runner_o_cells = set(o_runner['o_cell_id'])
runner_d_cells = set(d_runner['d_cell_id'])
print(f"  러너코스 O_CELL: {len(runner_o_cells):,}개  /  D_CELL: {len(runner_d_cells):,}개")


# ════════════════════════════════════════════════════════════
# Step 5. 격자 목록 통합 + 겹치는 격자 처리
# ════════════════════════════════════════════════════════════
print("\n▶ Step5. 격자 목록 통합 중...")

nature_o_cells = park_o_cells | runner_o_cells
nature_d_cells = park_d_cells | runner_d_cells

# 역 버퍼 ∩ 공원/러너 겹치는 격자 → 공원/러너 우선
subway_o_cells = subway_o_cells - nature_o_cells
subway_d_cells = subway_d_cells - nature_d_cells
o_cell_station_map = o_cell_station_map[o_cell_station_map['o_cell_id'].isin(subway_o_cells)]

# D_CELL 목적지 매핑 테이블 (공원 + 러너코스 통합)
d_cell_dest_map = pd.concat([
    d_park[['d_cell_id','공원명','공원종류']],
    d_runner[['d_cell_id','공원명','공원종류']]
], ignore_index=True).drop_duplicates('d_cell_id')

print(f"  역 O_CELL: {len(subway_o_cells):,}개  /  공원+러너 D_CELL: {len(nature_d_cells):,}개")


# ── 매핑 정보 저장 ──────────────────────────────────────────
o_cell_station_map.to_csv(os.path.join(OUTPUT_DIR, "mapping_subway_cells.csv"), index=False, encoding='utf-8-sig')
d_cell_dest_map.to_csv(os.path.join(OUTPUT_DIR, "mapping_dest_cells.csv"), index=False, encoding='utf-8-sig')
print(f"  매핑 테이블 저장 완료")


# ════════════════════════════════════════════════════════════
# Step 6. 생활이동 데이터 필터링 및 저장
# ════════════════════════════════════════════════════════════
def process_files(file_type):
    files = sorted(glob.glob(os.path.join(MOVEMENT_DIR, f"PURPOSE250M_{file_type}_*.csv")))
    print(f"\n▶ {file_type} 파일 {len(files)}개 처리 중...")

    all_results = []

    for f in files:
        fname = os.path.basename(f)
        df = pd.read_csv(f, low_memory=False)

        # 숫자 ID 제거
        df = df[~df['o_cell_id'].astype(str).str.match(r'^\d+$')]
        df = df[~df['d_cell_id'].astype(str).str.match(r'^\d+$')]

        # 양방향 필터링
        df_sub_to_nature = df[
            df['o_cell_id'].isin(subway_o_cells) &
            df['d_cell_id'].isin(nature_d_cells) &
            ~df['d_cell_id'].isin(subway_d_cells)
        ]
        df_nature_to_sub = df[
            df['o_cell_id'].isin(nature_o_cells) &
            df['d_cell_id'].isin(subway_d_cells) &
            ~df['o_cell_id'].isin(subway_o_cells)
        ]

        combined = pd.concat([df_sub_to_nature, df_nature_to_sub], ignore_index=True)

        # 시간대 재분류
        combined['time_band'] = reclassify_time_band(combined['time_band'])

        # 재집계
        combined = (
            combined
            .groupby(['o_cell_id','o_cell_x','o_cell_y',
                      'd_cell_id','d_cell_x','d_cell_y','time_band'], observed=True)
            ['total_pop'].sum().reset_index()
        )

        all_results.append(combined)
        print(f"  {fname}: {len(df):,}행 → {len(combined):,}행")

    print(f"  전체 합산 중...")
    final_grid = (
        pd.concat(all_results, ignore_index=True)
        .groupby(['o_cell_id','o_cell_x','o_cell_y',
                  'd_cell_id','d_cell_x','d_cell_y','time_band'], observed=True)
        ['total_pop'].sum().reset_index()
    )
    final_grid = final_grid.sort_values(['o_cell_id','d_cell_id','time_band']).reset_index(drop=True)

    # ── 파일 3/4: 격자별 집계 저장 (MCLP용) ──────────────────
    out_grid = os.path.join(OUTPUT_DIR, f"PURPOSE250M_{file_type}_filtered.csv")
    final_grid.to_csv(out_grid, index=False, encoding='utf-8-sig')
    print(f"  격자별 저장 완료: {out_grid}  ({final_grid.shape[0]:,}행)")

    # ── 파일 1/2: 역별 집계 저장 (분석용) ────────────────────
    # O_CELL → 역사명 매핑
    final_station = final_grid.merge(
        o_cell_station_map[['o_cell_id','역사명','호선']],
        on='o_cell_id', how='left'
    )
    # D_CELL → 목적지명 매핑
    final_station = final_station.merge(
        d_cell_dest_map[['d_cell_id','공원명','공원종류']],
        on='d_cell_id', how='left'
    )

    # 역별 + 목적지별 + 시간대별 집계
    final_station = (
        final_station
        .groupby(['역사명','호선','공원명','공원종류','time_band'], observed=True)
        ['total_pop'].sum().reset_index()
    )
    final_station = final_station.sort_values(['역사명','공원명','time_band']).reset_index(drop=True)

    out_station = os.path.join(OUTPUT_DIR, f"final_{file_type}.csv")
    final_station.to_csv(out_station, index=False, encoding='utf-8-sig')
    print(f"  역별 저장 완료: {out_station}  ({final_station.shape[0]:,}행)")


process_files("departure")
process_files("arrival")

print("\n✅ 완료")
