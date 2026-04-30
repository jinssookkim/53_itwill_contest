import pandas as pd
import glob
import os

files = sorted(glob.glob('파일경로/PURPOSE_in_250M_202509*.csv'))
print(f'총 {len(files)}개 파일 발견')

def time_band(t):
    if 5 <= t <= 7:
        return '05~08시'
    elif 8 <= t <= 17:
        return '08~18시'
    elif 18 <= t <= 21:
        return '18~22시'
    else:
        return '22~05시'

for f in files:
    filename = os.path.basename(f)
    print(f'처리 중: {filename}')
    
    result_list = []
    for chunk in pd.read_csv(f, encoding='utf-8', chunksize=500000):
        chunk['시간대'] = chunk['st_time_cd'].apply(time_band)
        agg = chunk.groupby(
            ['o_cell_id','o_cell_x','o_cell_y','d_cell_id','d_cell_x','d_cell_y','시간대']
        )['total_cnt'].sum().reset_index()
        result_list.append(agg)
        del chunk

    print('청크 합산 중...')
    final = pd.concat(result_list).groupby(
        ['o_cell_id','o_cell_x','o_cell_y','d_cell_id','d_cell_x','d_cell_y','시간대']
    )['total_cnt'].sum().reset_index()

    final = final.pivot_table(
        index=['o_cell_id','o_cell_x','o_cell_y','d_cell_id','d_cell_x','d_cell_y'],
        columns='시간대',
        values='total_cnt',
        fill_value=0
    ).reset_index()

    final.columns.name = None

    for col in ['05~08시','08~18시','18~22시','22~05시']:
        if col not in final.columns:
            final[col] = 0

    final['total_cnt'] = final[['05~08시','08~18시','18~22시','22~05시']].sum(axis=1)

    out_name = filename.replace('.csv', '_집계.csv')
    final.to_csv(r'E:\202509_수도권생활이동데이터집계\\' + out_name, index=False, encoding='utf-8')

    del result_list, final
    print(f'저장완료: {out_name}')

print('전체 완료')