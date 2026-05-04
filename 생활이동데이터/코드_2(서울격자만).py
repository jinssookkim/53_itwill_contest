import pandas as pd
import geopandas as gpd

# 서울 격자 ID 목록(이거 파일경로 수정)
gdf = gpd.read_file(r'C:\Users\Kim dong gwan\Desktop\생활이동데이터\match.shp')
seoul_cells = set(gdf['CELL_ID'].tolist())
print('서울 격자 수:', len(seoul_cells))

# 최종 합산 파일 청크로 읽으면서 필터(파일경로 수정해야함)
result_list = []
for chunk in pd.read_csv(r'C:\Users\Kim dong gwan\Desktop\생활이동데이터\최종_9월합산.csv', 
                          chunksize=100000, low_memory=False):
    filtered = chunk[
        chunk['o_cell_id'].astype(str).isin(seoul_cells) & 
        chunk['d_cell_id'].astype(str).isin(seoul_cells)
    ]
    result_list.append(filtered)
    del chunk

final = pd.concat(result_list)
print('서울 내 이동 행수:', len(final))

# 이것도 파일경로 수정
final.to_csv(r'C:\Users\Kim dong gwan\Desktop\생활이동데이터\최종_9월_서울필터.csv', 
             index=False, encoding='utf-8')
print('저장완료')