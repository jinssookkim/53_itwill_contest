"""
시간대 합산 스크립트
- 4개 filtered 파일에서 time_band 제거 후 total_pop 합산
- 각각 _total.csv 파일로 저장
"""

import pandas as pd
import os

# ── 경로 설정 (필요 시 수정) ────────────────────────────────
OUTPUT_DIR = r"D:\output\filtered"
# ────────────────────────────────────────────────────────────

files = {
    # 입력 파일 : groupby 키
    r"D:\output\filtered\PURPOSE250M_departure_filtered.csv": [
        'o_cell_id','o_cell_x','o_cell_y',
        'd_cell_id','d_cell_x','d_cell_y'
    ],
    r"D:\output\filtered\PURPOSE250M_arrival_filtered.csv": [
        'o_cell_id','o_cell_x','o_cell_y',
        'd_cell_id','d_cell_x','d_cell_y'
    ],
    r"D:\output\filtered\final_departure.csv": [
        '역사명','호선','공원명','공원종류'
    ],
    r"D:\output\filtered\final_arrival.csv": [
        '역사명','호선','공원명','공원종류'
    ],
}

for fname, group_keys in files.items():
    in_path = os.path.join(OUTPUT_DIR, fname)
    df = pd.read_csv(in_path, low_memory=False)
    print(f"\n▶ {fname}")
    print(f"  변환 전: {len(df):,}행")

    df_total = (
        df.groupby(group_keys, observed=True)
        ['total_pop'].sum().reset_index()
    )
    df_total = df_total.sort_values(group_keys).reset_index(drop=True)

    out_name = fname.replace('.csv', '_total.csv')
    out_path = os.path.join(OUTPUT_DIR, out_name)
    df_total.to_csv(out_path, index=False, encoding='utf-8-sig')
    print(f"  변환 후: {len(df_total):,}행")
    print(f"  저장 완료: {out_path}")

print("\n✅ 완료")
