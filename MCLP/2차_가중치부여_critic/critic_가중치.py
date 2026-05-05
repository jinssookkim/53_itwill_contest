import pandas as pd
import numpy as np

df = pd.read_csv("격자별_변수_통합.csv")

# 사용할 변수들
cols = ['생활이동인구', '스트라바이용자수', '거주인구', '녹시율', '공원겹침비율']

# 1. log1p 변환 (생활이동인구, 스트라바)
df['생활이동인구_log'] = np.log1p(df['생활이동인구'])
df['스트라바이용자수_log'] = np.log1p(df['스트라바이용자수'])

# 2. Min-Max 정규화
def minmax(series):
    return (series - series.min()) / (series.max() - series.min())

df['생활이동인구_norm'] = minmax(df['생활이동인구_log'])
df['스트라바이용자수_norm'] = minmax(df['스트라바이용자수_log'])
df['거주인구_norm'] = minmax(df['거주인구'])
df['녹시율_norm'] = df['녹시율']
df['공원겹침비율_norm'] = df['공원겹침비율']

norm_cols = ['생활이동인구_norm', '스트라바이용자수_norm', '거주인구_norm', '녹시율_norm', '공원겹침비율_norm']
X = df[norm_cols].values

# 3. CRITIC 가중치 계산
# 표준편차
std = X.std(axis=0)

# 상관관계 행렬
corr = np.corrcoef(X.T)

# 충돌성: Σ(1 - r_jk) for k≠j
conflict = np.array([sum(1 - corr[j, k] for k in range(len(norm_cols)) if k != j)
                     for j in range(len(norm_cols))])

# 정보량 = 표준편차 × 충돌성
info = std * conflict

# 가중치 = 정보량 / 합계
weights = info / info.sum()

print("CRITIC 가중치:")
for col, w in zip(norm_cols, weights):
    print(f"  {col}: {w:.4f}")

# 4. 수요점수 계산
df['수요점수_critic'] = sum(df[col] * w for col, w in zip(norm_cols, weights))

result = df[['CELL_ID', '위도', '경도'] + norm_cols + ['수요점수_critic']]
result.to_csv("격자별_수요점수_critic.csv", index=False, encoding='utf-8-sig')
print("\n완료")
print(result[['수요점수_critic']].describe())