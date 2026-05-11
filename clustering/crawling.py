# =========================================================
# 0. 라이브러리 불러오기
# =========================================================
import pandas as pd
import numpy as np

from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score, adjusted_rand_score

from scipy.stats import kruskal
import matplotlib.pyplot as plt

# Dunn test용
# scikit_posthocs가 설치되어 있지 않으면 아래 한 번 실행
# !pip install scikit-posthocs
import scikit_posthocs as sp

# =========================================================
# 1. 데이터 불러오기
# =========================================================
df = pd.read_csv("station_cluster_weighted_800m.csv")

cluster_vars = [
    "공원수",
    "공원면적합산(㎡)",
    "도시자연공원수",
    "한강공원수",
    "daily_avg",
    "보정_이용자수합계"
]

print("데이터 크기:", df.shape)
print("\n결측치 확인")
print(df[cluster_vars].isna().sum())

df[cluster_vars].describe()

# =========================================================
# 2. 클러스터링용 데이터 전처리
#    - 공원수, daily_avg, 이용자수합계_raw만 log1p 적용
#    - 이후 모든 변수에 StandardScaler 적용
# =========================================================

X = df[cluster_vars].copy()

# 결측치가 있으면 0으로 처리
# 필요하면 평균/중앙값 대체로 바꿀 수도 있음
X = X.fillna(0)

# 로그 변환할 변수만 지정
log_vars = [
    "공원수",
    "daily_avg",
    "보정_이용자수합계"
]

X_log = X.copy()

for col in log_vars:
    X_log[col] = np.log1p(X_log[col])

# StandardScaler는 모든 클러스터링 변수에 적용
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X_log)

X_scaled_df = pd.DataFrame(X_scaled, columns=cluster_vars)

print("로그 변환 적용 변수:", log_vars)
print("StandardScaler 적용 변수:", cluster_vars)

X_scaled_df.head()

# =========================================================
# 2-1. 로그 전후 왜도 확인
# =========================================================

skew_compare = pd.DataFrame({
    "original_skew": X[cluster_vars].skew(),
    "after_log_selected_skew": X_log[cluster_vars].skew()
}).round(3)

skew_compare

# =========================================================
# 3. 적절한 군집 수 확인: Elbow + Silhouette
# =========================================================

inertias = []
silhouette_scores = []

K_range = range(2, 8)

for k in K_range:
    kmeans = KMeans(n_clusters=k, random_state=42, n_init=20)
    labels = kmeans.fit_predict(X_scaled)

    inertias.append(kmeans.inertia_)
    silhouette_scores.append(silhouette_score(X_scaled, labels))

# 결과표 생성
k_result_df = pd.DataFrame({
    "k": list(K_range),
    "inertia": inertias,
    "silhouette_score": silhouette_scores
})

# k가 1 증가할 때 inertia가 얼마나 줄었는지
k_result_df["inertia_decrease"] = k_result_df["inertia"].shift(1) - k_result_df["inertia"]

# 감소율: 이전 k 대비 몇 % 줄었는지
k_result_df["inertia_decrease_rate(%)"] = (
    k_result_df["inertia_decrease"] / k_result_df["inertia"].shift(1) * 100
)

# 보기 좋게 반올림
k_result_df = k_result_df.round({
    "inertia": 3,
    "silhouette_score": 4,
    "inertia_decrease": 3,
    "inertia_decrease_rate(%)": 2
})

display(k_result_df)

# Elbow plot
plt.figure(figsize=(6, 4))
plt.plot(k_result_df["k"], k_result_df["inertia"], marker="o")
plt.xlabel("Number of clusters (k)")
plt.ylabel("Inertia")
plt.title("Elbow Method")
plt.grid(alpha=0.3)
plt.show()

# Silhouette plot
plt.figure(figsize=(6, 4))
plt.plot(k_result_df["k"], k_result_df["silhouette_score"], marker="o")
plt.xlabel("Number of clusters (k)")
plt.ylabel("Silhouette Score")
plt.title("Silhouette Score by k")
plt.grid(alpha=0.3)
plt.show()

# =========================================================
# 4. KMeans 클러스터링 실행
#    그래프 보고 best_k 수정
# =========================================================

best_k = 4

kmeans = KMeans(n_clusters=best_k, random_state=42, n_init=20)
df["cluster"] = kmeans.fit_predict(X_scaled)

df[["역사명", "호선", "cluster"] + cluster_vars].head()

# =========================================================
# 4-1. ARI: k 변화에 따른 군집 구조 유사성 확인
# =========================================================

kmeans_3 = KMeans(n_clusters=3, random_state=42, n_init=20)
labels_3 = kmeans_3.fit_predict(X_scaled)

kmeans_4 = KMeans(n_clusters=4, random_state=42, n_init=20)
labels_4 = kmeans_4.fit_predict(X_scaled)

kmeans_5 = KMeans(n_clusters=5, random_state=42, n_init=20)
labels_5 = kmeans_5.fit_predict(X_scaled)

ari_k3_k4 = adjusted_rand_score(labels_3, labels_4)
ari_k4_k5 = adjusted_rand_score(labels_4, labels_5)

print("ARI (k=3 vs k=4):", ari_k3_k4)
print("ARI (k=4 vs k=5):", ari_k4_k5)

# =========================================================
# 5. 군집별 기초 통계 확인
#    해석은 원본 변수 기준으로 보는 것이 좋음
# =========================================================

cluster_summary = df.groupby("cluster")[cluster_vars].agg(
    ["mean", "median", "min", "max", "count"]
)

cluster_summary

# 보기 편한 평균표
cluster_mean = df.groupby("cluster")[cluster_vars].mean().round(2)
cluster_mean

# 보기 편한 중앙값표
cluster_median = df.groupby("cluster")[cluster_vars].median().round(2)
cluster_median

# =========================================================
# 6. 결과 저장
# =========================================================

df.to_csv("station_cluster_result_log_selected_scaled_weighted_800m.csv", index=False, encoding="utf-8-sig")
cluster_mean.to_csv("cluster_mean_summary_log_selected_scaled_weighted_800m.csv", encoding="utf-8-sig")
cluster_median.to_csv("cluster_median_summary_log_selected_scaled_weighted_800m.csv", encoding="utf-8-sig")

print("저장 완료")

# =========================================================
# 7. Kruskal-Wallis 검정
#    목적: 군집별로 각 변수가 통계적으로 다르게 분포하는지 확인
#    해석은 원본 변수 기준으로 진행
# =========================================================

kw_vars = [
    "공원수",
    "공원면적합산(㎡)",
    "도시자연공원수",
    "한강공원수",
    "daily_avg",
    "보정_이용자수합계"
]

cluster_col = "cluster"
clusters = sorted(df[cluster_col].dropna().unique())

cluster_name_map = {
    c: f"군집 {i+1}" for i, c in enumerate(clusters)
}

result_rows = []

# 1) 군집별 평균, 표준편차
for c in clusters:
    row = {
        "군집 유형": cluster_name_map[c],
        "표본 수(N)": df.loc[df[cluster_col] == c].shape[0]
    }

    for var in kw_vars:
        values = df.loc[df[cluster_col] == c, var].dropna()

        row[(var, "평균")] = values.mean()
        row[(var, "표준편차")] = values.std()

    result_rows.append(row)

# 2) 평균순위 Ri
for var in kw_vars:
    temp = df[[cluster_col, var]].dropna().copy()
    temp["rank"] = rankdata(temp[var], method="average")
    mean_rank = temp.groupby(cluster_col)["rank"].mean()

    for i, c in enumerate(clusters):
        result_rows[i][(var, "Rᵢ")] = mean_rank.loc[c]

# 3) Kruskal-Wallis H 통계량과 p-value
h_row = {
    "군집 유형": "χ²",
    "표본 수(N)": "-"
}

p_row = {
    "군집 유형": "sig",
    "표본 수(N)": "-"
}

for var in kw_vars:
    groups = [
        df.loc[df[cluster_col] == c, var].dropna()
        for c in clusters
    ]

    h_stat, p_value = kruskal(*groups)

    h_row[(var, "평균")] = h_stat
    h_row[(var, "표준편차")] = ""
    h_row[(var, "Rᵢ")] = ""

    if p_value < 0.001:
        p_text = "p < .001"
    elif p_value < 0.01:
        p_text = "p < .01"
    elif p_value < 0.05:
        p_text = "p < .05"
    else:
        p_text = f"p = {p_value:.3f}"

    p_row[(var, "평균")] = p_text
    p_row[(var, "표준편차")] = ""
    p_row[(var, "Rᵢ")] = ""

result_rows.append(h_row)
result_rows.append(p_row)

# 4) 표 만들기
kw_table = pd.DataFrame(result_rows)

ordered_cols = ["군집 유형", "표본 수(N)"]

for var in kw_vars:
    ordered_cols.extend([
        (var, "평균"),
        (var, "표준편차"),
        (var, "Rᵢ")
    ])

kw_table = kw_table[ordered_cols]

# 5) 반올림
kw_table_display = kw_table.copy()

for col in kw_table_display.columns:
    if col not in ["군집 유형", "표본 수(N)"]:
        kw_table_display[col] = kw_table_display[col].apply(
            lambda x: round(x, 3) if isinstance(x, (int, float, np.float64)) else x
        )

display(kw_table_display)

kw_table_display.to_csv(
    "kruskal_wallis_cluster_summary_table.csv",
    index=False,
    encoding="utf-8-sig"
)

print("저장 완료")