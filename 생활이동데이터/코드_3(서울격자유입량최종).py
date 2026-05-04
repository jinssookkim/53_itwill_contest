import pandas as pd

# 파일 경로 수정해야함
df = pd.read_csv(r'C:\Users\Kim dong gwan\Desktop\생활이동데이터\최종_9월_서울필터.csv', low_memory=False)

# 출발량
outflow = df.groupby('o_cell_id')['total_cnt'].sum().reset_index()
outflow.columns = ['CELL_ID', 'outflow']

# 유입량
inflow = df.groupby('d_cell_id')['total_cnt'].sum().reset_index()
inflow.columns = ['CELL_ID', 'inflow']

# 합치기
result = outflow.merge(inflow, on='CELL_ID', how='outer').fillna(0)
result['total'] = result['outflow'] + result['inflow']
result = result.sort_values('total', ascending=False)

print(result.head(20))
# 파일경로 수정해야함
result.to_csv(r'C:\Users\Kim dong gwan\Desktop\생활이동데이터\격자별_이동량.csv', index=False, encoding='utf-8')
print('저장완료, 행수:', len(result))