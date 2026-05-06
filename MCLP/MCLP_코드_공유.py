# 라이브러리 설치 필요!
# pip install pandas numpy geopandas shapely pulp (이거 해야함)

# 1. 상단 설정 및 라이브러리
"""
펀스테이션 입지선정 MCLP 분석 코드
- 목적함수: max Σ aᵢ × cᵢ
- Gravity 가중치: wᵢⱼ = (1 - d/800)², 800m 초과 = 0 (거리가 멀수록 점수 덜 줌)
- 자치구당 최대 2개 제약
"""

import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Point
from pulp import *

# 여기만 바꾸면 됨
S = 800        # 커버리지 반경 (미터)
P = 30         # 설치할 펀스테이션 개수
MAX_PER_GU = 2 # 자치구당 최대 설치 개수

# 파일 읽기
df = pd.read_csv("격자별_변수_통합.csv")
station = pd.read_csv("MCLP_최종후보역.csv")
gu = gpd.read_file("서울_자치구_경계_2017.geojson")

# 2. 기존 역 제외
# 이미 설치된 펀스테이션 역 제외
existing = ['뚝섬', '여의나루', '광화문(세종문화회관)', '회현(남대문시장)', '월드컵경기장(성산)', '먹골']
existing_coords = station[station['역사명'].isin(existing)][['역사명', '위도', '경도']]
station = station[~station['역사명'].isin(existing)].reset_index(drop=True)
print(f"후보역 수: {len(station)}개")

# 3. 수요점수 전처리
# log1p: 이상치 심한 변수 로그 변환 (극단값이 Min-Max 지배 방지)
df['생활이동인구_log'] = np.log1p(df['생활이동인구'])
df['스트라바이용자수_log'] = np.log1p(df['스트라바이용자수'])

# Min-Max 정규화: 0~1로 변환 (변수간 단위 차이 제거)
def minmax(series):
    return (series - series.min()) / (series.max() - series.min())

df['생활이동인구_norm'] = minmax(df['생활이동인구_log'])
df['스트라바이용자수_norm'] = minmax(df['스트라바이용자수_log'])
df['거주인구_norm'] = minmax(df['거주인구'])
df['녹시율_norm'] = df['녹시율']           # 이미 0~1
df['공원겹침비율_norm'] = df['공원겹침비율'] # 이미 0~1

# 4. 가중치 계산
norm_cols = ['생활이동인구_norm', '스트라바이용자수_norm', '거주인구_norm', '녹시율_norm', '공원겹침비율_norm']
X = df[norm_cols].values
n = len(X)

# CRITIC: 표준편차(변별력) × 충돌성(다른 변수와 독립성) → 가중치
std = X.std(axis=0)
corr_mat = np.corrcoef(X.T)
conflict = np.array([sum(1 - corr_mat[j, k] for k in range(5) if k != j) for j in range(5)])
critic_w = (std * conflict) / (std * conflict).sum()

# IEW: 엔트로피 낮을수록(격자간 차이 클수록) 높은 가중치
k_ent = 1 / np.log(n)
P_mat = X / X.sum(axis=0)
P_mat = np.where(P_mat == 0, 1e-10, P_mat)
E = -k_ent * (P_mat * np.log(P_mat)).sum(axis=0)
iew_w = (1 - E) / (1 - E).sum()

scenarios = {
    '동일가중치': [0.2, 0.2, 0.2, 0.2, 0.2],
    'CRITIC': critic_w.tolist(),
    'IEW': iew_w.tolist(),
}

# 5. 기존 역 800m 격자 제외 + 거리 행렬
def haversine(lat1, lon1, lat2, lon2):
    # 두 좌표 사이 거리 계산 (미터)
    R = 6371000
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat, dlon = lat2-lat1, lon2-lon1
    a = np.sin(dlat/2)**2 + np.cos(lat1)*np.cos(lat2)*np.sin(dlon/2)**2
    return R * 2 * np.arcsin(np.sqrt(a))

# 기존 역 800m 안 격자 제외 (이미 커버된 수요지점 제거)
mask = pd.Series([True]*len(df))
for _, row in existing_coords.iterrows():
    dist = df.apply(lambda r: haversine(r['위도'], r['경도'], row['위도'], row['경도']), axis=1)
    mask = mask & (dist > S)
df = df[mask].reset_index(drop=True)
print(f"수요지점 수: {len(df)}개")

def haversine_matrix(glat, glon, slat, slon):
    # 모든 역-격자 거리 행렬 한번에 계산 (속도 최적화)
    R = 6371000
    glat, glon = np.radians(glat), np.radians(glon)
    slat, slon = np.radians(slat), np.radians(slon)
    dlat = slat[:, None] - glat[None, :]
    dlon = slon[:, None] - glon[None, :]
    a = np.sin(dlat/2)**2 + np.cos(glat[None,:])*np.cos(slat[:,None])*np.sin(dlon/2)**2
    return R * 2 * np.arcsin(np.sqrt(a))

print("거리 행렬 계산 중...")
dist_matrix = haversine_matrix(
    df['위도'].values, df['경도'].values,
    station['위도'].values, station['경도'].values
).T

# Gravity 가중치: wᵢⱼ = (1-d/800)², 800m 초과 = 0
w_matrix = np.where(dist_matrix <= S, (1 - dist_matrix/S)**2, 0)

# 6. 자치구 매핑 + MCLP 실행
# 자치구 매핑 (자치구당 최대 2개 제약을 위해)
station_gdf = gpd.GeoDataFrame(
    station,
    geometry=[Point(lon, lat) for lon, lat in zip(station['경도'], station['위도'])],
    crs="EPSG:4326"
)
gu = gu.to_crs(epsg=4326)
station_with_gu = gpd.sjoin(station_gdf, gu[['SIG_KOR_NM','geometry']], how='left', predicate='within')
station['자치구'] = station_with_gu['SIG_KOR_NM'].values

I, J = len(df), len(station)
results = {}

for name, weights in scenarios.items():
    print(f"\n[{name}] MCLP 최적화 중...")

    # 수요점수 계산: 5개 변수 × 가중치 합산
    a = (df['생활이동인구_norm']*weights[0] + df['스트라바이용자수_norm']*weights[1] +
         df['거주인구_norm']*weights[2] + df['녹시율_norm']*weights[3] +
         df['공원겹침비율_norm']*weights[4]).values

    prob = LpProblem(f"MCLP_{name}", LpMaximize)
    y = [LpVariable(f"y_{name}_{j}", cat='Binary') for j in range(J)]  # 역 선택 여부
    c = [LpVariable(f"c_{name}_{i}", lowBound=0, upBound=1) for i in range(I)]  # 격자 커버수준

    # 목적함수: max Σ aᵢ × cᵢ
    prob += lpSum(a[i]*c[i] for i in range(I))

    # 제약1: cᵢ ≤ Σ wᵢⱼ × yⱼ (선택된 역만 격자 커버 가능)
    for i in range(I):
        covering_js = [j for j in range(J) if w_matrix[i,j] > 0]
        if covering_js:
            prob += c[i] <= lpSum(w_matrix[i,j]*y[j] for j in covering_js)
        else:
            prob += c[i] == 0

    # 제약2: Σ yⱼ = P (P개만 선택)
    prob += lpSum(y) == P

    # 제약3: 자치구당 최대 2개 (형평성)
    for gu_name in station['자치구'].dropna().unique():
        gu_js = [j for j in range(J) if station.iloc[j]['자치구'] == gu_name]
        if gu_js:
            prob += lpSum(y[j] for j in gu_js) <= MAX_PER_GU

    prob.solve(PULP_CBC_CMD(msg=0))

    selected = station[[v.varValue==1 for v in y]][['역사명','호선','자치구','cluster_label']].copy()
    selected = selected.reset_index(drop=True)
    selected.index += 1
    results[name] = set(selected['역사명'])
    print(f"[{name}] P={P} 결과:")
    print(selected.to_string())

# 결과 비교
print("\n" + "="*60)
print("모든 시나리오 공통 핵심역:")
for r in sorted(set.intersection(*results.values())):
    print(f"  {r}")

print("\n시나리오별 고유 역:")
for name, sel in results.items():
    unique = sel - set.union(*[v for k,v in results.items() if k!=name])
    print(f"\n  [{name}] 고유역: {sorted(unique)}")