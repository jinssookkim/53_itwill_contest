import pandas as pd
import numpy as np

df = pd.read_csv("격자별_변수_통합.csv")

# log1p 변환
df['생활이동인구_log'] = np.log1p(df['생활이동인구'])
df['스트라바이용자수_log'] = np.log1p(df['스트라바이용자수'])

# Min-Max 정규화
def minmax(series):
    return (series - series.min()) / (series.max() - series.min())

df['생활이동인구_norm'] = minmax(df['생활이동인구_log'])
df['스트라바이용자수_norm'] = minmax(df['스트라바이용자수_log'])
df['거주인구_norm'] = minmax(df['거주인구'])
df['녹시율_norm'] = df['녹시율']
df['공원겹침비율_norm'] = df['공원겹침비율']

# aᵢ 계산
df['ai'] = (df['생활이동인구_norm'] + df['스트라바이용자수_norm'] + 
            df['거주인구_norm'] + df['녹시율_norm'] + df['공원겹침비율_norm'])

# 필요한 컬럼만
result = df[['CELL_ID', '위도', '경도', 
             '생활이동인구_norm', '스트라바이용자수_norm', 
             '거주인구_norm', '녹시율_norm', '공원겹침비율_norm', 'ai']]

result = result.rename(columns={'ai': '수요점수'})

result.to_csv("격자별_수요점수.csv", index=False, encoding='utf-8-sig')
print("완료")
print(result.describe())