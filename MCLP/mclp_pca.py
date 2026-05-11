import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Point
from pulp import *
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from scipy import stats

# 🔥 여기 추가 (무조건 위)
def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return R * 2 * np.arcsin(np.sqrt(a))

# 파일 읽기
df = pd.read_csv(r"C:\Users\JS Kim\Downloads\격자별_변수_통합_수정_pop.csv", encoding='utf-8-sig')
station = pd.read_csv(r"C:\Users\JS Kim\Downloads\MCLP에 넣는 후보역들_186개.csv", encoding='utf-8-sig')
gu = gpd.read_file(r"C:\Users\JS Kim\Downloads\서울_자치구_경계_2017.geojson")

S = 800
P = 15
MAX_PER_GU = 2

# 기존 역 6개 제외
# existing = ['뚝섬', '여의나루', '광화문(세종문화회관)', '회현(남대문시장)', '월드컵경기장(성산)', '먹골']
# existing_coords = station[station['역사명'].isin(existing)][['역사명', '위도', '경도']]
# print("제외할 기존 역:")
# print(existing_coords)
# station = station[~station['역사명'].isin(existing)].reset_index(drop=True)
# print("후보역 수:", len(station))

# 왜도 확인
skewness = stats.skew(df['생활이동'])
print(f"생활이동 왜도: {skewness:.3f}")

# 수요점수 변수 준비
df['생활이동_log'] = np.log1p(df['생활이동'])
print(stats.skew(df['생활이동_log']))
df['스트라바이용자수_log'] = np.log1p(df['스트라바이용자수'])

def minmax(series):
    return (series - series.min()) / (series.max() - series.min())

df['생활이동_norm'] = minmax(df['생활이동_log'])
df['스트라바이용자수_norm'] = minmax(df['스트라바이용자수_log'])
df['거주인구_norm'] = minmax(df['거주인구'])
df['녹시율_norm'] = df['녹시율']
df['공원겹침비율_norm'] = df['공원겹침비율']

# 생활이동 & 스트라바이용자수 → 상관계수 높아 PCA로 합성
_X_pca = StandardScaler().fit_transform(df[['생활이동_norm', '스트라바이용자수_norm']])
_pca = PCA(n_components=1)
_pc1 = _pca.fit_transform(_X_pca)[:, 0]
print(f"[PCA] 생활이동 + 스트라바이용자수 설명분산: {_pca.explained_variance_ratio_[0]:.3f}")
print(f"[PCA] 성분 계수: 생활이동={_pca.components_[0][0]:.3f}, 스트라바={_pca.components_[0][1]:.3f}")
# 계수가 둘 다 양수인지 확인 (음수면 방향 반전)
if _pca.components_[0].mean() < 0:
    _pc1 *= -1
df['이용수요_PC1_norm'] = minmax(pd.Series(_pc1))

norm_cols = ['이용수요_PC1_norm', '거주인구_norm', '녹시율_norm', '공원겹침비율_norm']
X = df[norm_cols].values
n = len(X)

# CRITIC 가중치
std = X.std(axis=0)
corr_mat = np.corrcoef(X.T)
conflict = np.array([sum(1 - corr_mat[j, k] for k in range(4) if k != j) for j in range(4)])
info = std * conflict
critic_w = info / info.sum()

# IEW 가중치
k = 1 / np.log(n)
X_sum = X.sum(axis=0)
P_mat = X / X_sum
P_mat = np.where(P_mat == 0, 1e-10, P_mat)
E = -k * (P_mat * np.log(P_mat)).sum(axis=0)
iew_w = (1 - E) / (1 - E).sum()

scenarios = {
    '동일가중치': [0.2, 0.2, 0.2, 0.2],
    'CRITIC': critic_w.tolist(),
    'IEW': iew_w.tolist(),
}

print("\n가중치 비교:")
print(f"{'시나리오':<12} {'이용수요PC1':>12} {'거주인구':>10} {'녹시율':>10} {'공원겹침':>10}")
for name, w in scenarios.items():
    print(f"{name:<12} {w[0]:>12.4f} {w[1]:>10.4f} {w[2]:>10.4f} {w[3]:>10.4f}")

print("후보역 수:", len(station))
print("수요지점 수:", len(df))

# 기존 역 800m 안 격자 제외
# mask = pd.Series([True] * len(df))
# for _, row in existing_coords.iterrows():
#     dist = df.apply(lambda r: haversine(r['위도'], r['경도'], row['위도'], row['경도']), axis=1)
#     mask = mask & (dist > S)
# df = df[mask].reset_index(drop=True)
# print("수요지점 수:", len(df))


# 자치구 매핑
station_gdf = gpd.GeoDataFrame(
    station,
    geometry=[Point(lon, lat) for lon, lat in zip(station['경도'], station['위도'])],
    crs="EPSG:4326"
)
gu = gu.to_crs(epsg=4326)
station_with_gu = gpd.sjoin(station_gdf, gu[['SIG_KOR_NM', 'geometry']], how='left', predicate='within')
station['자치구'] = station_with_gu['SIG_KOR_NM'].values

def haversine_matrix(glat, glon, slat, slon):
    R = 6371000
    glat = np.radians(glat)
    glon = np.radians(glon)
    slat = np.radians(slat)
    slon = np.radians(slon)
    dlat = slat[:, None] - glat[None, :]
    dlon = slon[:, None] - glon[None, :]
    a = np.sin(dlat/2)**2 + np.cos(glat[None, :]) * np.cos(slat[:, None]) * np.sin(dlon/2)**2
    return R * 2 * np.arcsin(np.sqrt(a))

print("\n거리 행렬 계산 중...")
dist_matrix = haversine_matrix(
    df['위도'].values, df['경도'].values,
    station['위도'].values, station['경도'].values
).T
w_matrix = np.where(dist_matrix <= S, (1 - dist_matrix / S) ** 2, 0)

I = len(df)
J = len(station)

results = {}

for name, weights in scenarios.items():
    print(f"\n[{name}] MCLP 최적화 중...")
    a = (df['이용수요_PC1_norm'] * weights[0] +
        #  df['스트라바이용자수_norm'] * weights[1] +
         df['거주인구_norm'] * weights[1] +
         df['녹시율_norm'] * weights[2] +
         df['공원겹침비율_norm'] * weights[3]).values

    prob = LpProblem(f"MCLP_{name}", LpMaximize)
    y = [LpVariable(f"y_{name}_{j}", cat='Binary') for j in range(J)]
    c = [LpVariable(f"c_{name}_{i}", lowBound=0, upBound=1) for i in range(I)]

    prob += lpSum(a[i] * c[i] for i in range(I))

    for i in range(I):
        covering_js = [j for j in range(J) if w_matrix[i, j] > 0]
        if covering_js:
            prob += c[i] <= lpSum(w_matrix[i, j] * y[j] for j in covering_js)
        else:
            prob += c[i] == 0

    prob += lpSum(y) == P

    for gu_name in station['자치구'].dropna().unique():
        gu_js = [j for j in range(J) if station.iloc[j]['자치구'] == gu_name]
        if gu_js:
            prob += lpSum(y[j] for j in gu_js) <= MAX_PER_GU

    prob.solve(PULP_CBC_CMD(msg=0))

    selected = station[[v.varValue == 1 for v in y]][['역사명', '호선', '자치구', 'cluster_label']].copy()
    selected = selected.reset_index(drop=True)
    selected.index += 1
    results[name] = set(selected['역사명'])

    print(f"[{name}] P={P} 결과:")
    print(selected.to_string())

print("\n" + "="*60)
print("모든 시나리오 공통 역 (핵심역):")
common = set.intersection(*results.values())
for r in sorted(common):
    print(f"  {r}")

print("\n시나리오별 고유 역:")
for name, selected in results.items():
    unique = selected - set.union(*[v for k, v in results.items() if k != name])
    print(f"\n  [{name}] 고유역: {sorted(unique)}")
