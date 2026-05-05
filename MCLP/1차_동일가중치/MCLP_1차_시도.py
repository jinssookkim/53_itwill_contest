import pandas as pd
import numpy as np
from pulp import *

grid = pd.read_csv("격자별_수요점수.csv")
station = pd.read_csv("MCLP_최종후보역.csv")

S = 800
P = 15

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

print("거리 행렬 계산 중...")
dist_matrix = haversine_matrix(
    grid['위도'].values, grid['경도'].values,
    station['위도'].values, station['경도'].values
).T

w_matrix = np.where(dist_matrix <= S, (1 - dist_matrix / S) ** 2, 0)
a = grid['수요점수'].values

I = len(grid)
J = len(station)

print("MCLP 최적화 중...")
prob = LpProblem("MCLP_Gravity", LpMaximize)

# 변수
y = [LpVariable(f"y_{j}", cat='Binary') for j in range(J)]       # 역 선택 여부
c = [LpVariable(f"c_{i}", lowBound=0, upBound=1) for i in range(I)]  # 격자 커버 수준

# 목적함수: max Σ aᵢ × cᵢ
prob += lpSum(a[i] * c[i] for i in range(I))

# 제약 1: cᵢ ≤ Σ wᵢⱼ × yⱼ
for i in range(I):
    covering_js = [j for j in range(J) if w_matrix[i, j] > 0]
    if covering_js:
        prob += c[i] <= lpSum(w_matrix[i, j] * y[j] for j in covering_js)
    else:
        prob += c[i] == 0

# 제약 2: P개만 선택
prob += lpSum(y) == P

prob.solve(PULP_CBC_CMD(msg=1))

selected = station[[v.varValue == 1 for v in y]].copy().reset_index(drop=True)
selected['순위'] = range(1, len(selected) + 1)

print(f"\n선택된 역 {len(selected)}개:")
print(selected[['순위', '역사명', '호선', '위도', '경도']])
selected.to_csv("MCLP_결과.csv", index=False, encoding='utf-8-sig')
print("완료")