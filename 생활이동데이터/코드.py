import pandas as pd
import glob
import os

files = sorted(
    # 파일경로 수정해야함!
    glob.glob(r'C:\Users\Kim dong gwan\Desktop\생활이동데이터\202509_수도권생활이동데이터 집계.zip(16~20)\*_집계.csv')
)
print(f'총 {len(files)}개 파일 발견')

final = None

for f in files:
    print(f'읽는 중: {os.path.basename(f)}')
    
    chunk_list = []
    for chunk in pd.read_csv(f, encoding='utf-8', chunksize=100000, low_memory=False):
        agg = chunk.groupby(
            ['o_cell_id','o_cell_x','o_cell_y','d_cell_id','d_cell_x','d_cell_y']
        )[['05~08시','08~18시','18~22시','22~05시']].sum().reset_index()
        chunk_list.append(agg)
        del chunk
    
    df_agg = pd.concat(chunk_list).groupby(
        ['o_cell_id','o_cell_x','o_cell_y','d_cell_id','d_cell_x','d_cell_y']
    )[['05~08시','08~18시','18~22시','22~05시']].sum().reset_index()
    del chunk_list
    
    if final is None:
        final = df_agg
    else:
        final = pd.concat([final, df_agg]).groupby(
            ['o_cell_id','o_cell_x','o_cell_y','d_cell_id','d_cell_x','d_cell_y']
        )[['05~08시','08~18시','18~22시','22~05시']].sum().reset_index()
    
    del df_agg
    print(f'현재 행수: {len(final)}')

final['total_cnt'] = final[['05~08시','08~18시','18~22시','22~05시']].sum(axis=1)

# 여기 파일경로 수정
final.to_csv(r'C:\Users\Kim dong gwan\Desktop\생활이동데이터\최종_9월합산.csv', index=False, encoding='utf-8')
print('완료, 행수:', len(final))