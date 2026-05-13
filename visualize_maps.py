import sys
import os
import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import geopandas as gpd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import matplotlib.colors as mcolors
import matplotlib.cm as cm
import numpy as np
import polyline as polyline_lib

# ── 한글 폰트 설정 ──────────────────────────────────────────────
def set_korean_font():
    font_candidates = [
        'C:/Windows/Fonts/malgun.ttf',
        'C:/Windows/Fonts/malgunbd.ttf',
        'C:/Windows/Fonts/gulim.ttc',
        'C:/Windows/Fonts/NanumGothic.ttf',
    ]
    for path in font_candidates:
        if os.path.exists(path):
            font_prop = fm.FontProperties(fname=path)
            plt.rcParams['font.family'] = font_prop.get_name()
            fm.fontManager.addfont(path)
            plt.rcParams['axes.unicode_minus'] = False
            return font_prop.get_name()
    plt.rcParams['font.family'] = 'Malgun Gothic'
    plt.rcParams['axes.unicode_minus'] = False
    return 'Malgun Gothic'

font_name = set_korean_font()
print(f"Using font: {font_name}")

BASE = r'c:\Users\82106\Desktop\실험_MCLP_1차'

# ── 공통 데이터 로드 ────────────────────────────────────────────
seoul = gpd.read_file(os.path.join(BASE, '서울_자치구_경계_2017.geojson'))
if seoul.crs is None:
    seoul = seoul.set_crs(epsg=4326)
seoul_wgs = seoul.to_crs(epsg=4326)

print("서울 경계 로드 완료")

# ═══════════════════════════════════════════════════════════════
# 지도 1: 격자별 일평균 이동량
# ═══════════════════════════════════════════════════════════════
print("\n[지도 1] 격자별 일평균 이동량 시각화 시작...")

df_move = pd.read_csv(os.path.join(BASE, '격자별_최종_일평균_이동량_final.csv'), encoding='utf-8')
match = gpd.read_file(os.path.join(BASE, 'match.shp'))
print(f"  이동량 데이터: {df_move.shape}, match: {match.shape}")

# CELL_ID로 조인
merged = match.merge(df_move[['CELL_ID', '일평균_총이동']], on='CELL_ID', how='left')
merged = merged.dropna(subset=['일평균_총이동'])
print(f"  조인 결과: {merged.shape}")

# 좌표계 변환 (EPSG:5179 → 4326)
merged_wgs = merged.to_crs(epsg=4326)

# 서울 경계 bbox
xmin, ymin, xmax, ymax = seoul_wgs.total_bounds
margin = 0.02

fig, ax = plt.subplots(1, 1, figsize=(14, 12))
ax.set_facecolor('white')
fig.patch.set_facecolor('white')

# 서울 경계 (배경)
seoul_wgs.plot(ax=ax, color='#f5f5f5', edgecolor='#aaaaaa', linewidth=0.8, zorder=1)

# 이동량 분위수 기반 클리핑 (상위 1% 제거로 색상 선명하게)
vmin = merged_wgs['일평균_총이동'].quantile(0.1)
vmax = merged_wgs['일평균_총이동'].quantile(0.97)

# 흰색 배경에 잘 보이는 빨간색 계열 colormap
cmap = matplotlib.colors.LinearSegmentedColormap.from_list(
    'move_cmap',
    ['#ffffcc', '#fed976', '#fd8d3c', '#e31a1c', '#800026'],
    N=256
)

merged_wgs.plot(
    ax=ax,
    column='일평균_총이동',
    cmap=cmap,
    vmin=vmin,
    vmax=vmax,
    linewidth=0,
    alpha=0.9,
    zorder=2
)

# 서울 경계선만 위에 덧그리기
seoul_wgs.plot(ax=ax, color='none', edgecolor='#555555', linewidth=1.2, zorder=3)

# colorbar
sm = plt.cm.ScalarMappable(cmap=cmap, norm=plt.Normalize(vmin=vmin, vmax=vmax))
sm.set_array([])
cbar = fig.colorbar(sm, ax=ax, fraction=0.03, pad=0.02, shrink=0.7)
cbar.set_label('일평균 총이동량', fontsize=13, color='black', fontname=font_name)
cbar.ax.yaxis.set_tick_params(color='black')
plt.setp(cbar.ax.yaxis.get_ticklabels(), color='black', fontsize=10)

ax.set_xlim(xmin - margin, xmax + margin)
ax.set_ylim(ymin - margin, ymax + margin)
ax.set_title('서울시 격자별 일평균 이동량', fontsize=18, fontweight='bold',
             color='black', pad=15, fontname=font_name)
ax.set_axis_off()

plt.tight_layout()
out1 = os.path.join(BASE, '지도2_격자별_일평균이동량.png')
plt.savefig(out1, dpi=180, bbox_inches='tight', facecolor='white')
plt.close()
print(f"  저장: {out1}")


# ═══════════════════════════════════════════════════════════════
# 지도 2: 공원 경계 시각화
# ═══════════════════════════════════════════════════════════════
print("\n[지도 2] 공원 경계 시각화 시작...")

parks = gpd.read_file(os.path.join(BASE, '공원_경계_수정_최종본.geojson'))
if parks.crs is None:
    parks = parks.set_crs(epsg=4326)
parks_wgs = parks.to_crs(epsg=4326)
print(f"  공원 데이터: {parks_wgs.shape}")
print(f"  공원종류: {parks_wgs['공원종류'].unique().tolist()}")

# 강변공원 / 일반공원 분류
park_river = parks_wgs[parks_wgs['공원종류'] == '강변공원']
park_other = parks_wgs[parks_wgs['공원종류'] != '강변공원']
print(f"  강변공원: {len(park_river)}개, 일반공원: {len(park_other)}개")

fig, ax = plt.subplots(1, 1, figsize=(14, 12))
ax.set_facecolor('white')
fig.patch.set_facecolor('white')

# 서울 경계
seoul_wgs.plot(ax=ax, color='#f5f5f5', edgecolor='#aaaaaa', linewidth=1.0, zorder=1)

# 일반공원 (초록색)
if len(park_other) > 0:
    park_other.plot(
        ax=ax,
        color='#2e8b57',
        edgecolor='#1a5c35',
        linewidth=0.4,
        alpha=0.85,
        zorder=2
    )

# 강변공원 (파란색)
if len(park_river) > 0:
    park_river.plot(
        ax=ax,
        color='#1565c0',
        edgecolor='#0d47a1',
        linewidth=0.4,
        alpha=0.85,
        zorder=3
    )

# 서울 경계선 위에 덧그리기
seoul_wgs.plot(ax=ax, color='none', edgecolor='#555555', linewidth=1.2, zorder=4)

ax.set_xlim(xmin - margin, xmax + margin)
ax.set_ylim(ymin - margin, ymax + margin)
ax.set_title('서울시 공원 경계', fontsize=18, fontweight='bold',
             color='#1a1a1a', pad=15, fontname=font_name)
ax.set_axis_off()

# 범례
from matplotlib.patches import Patch
legend_elements = [
    Patch(facecolor='#2e8b57', edgecolor='#1a5c35', label='공원'),
    Patch(facecolor='#1565c0', edgecolor='#0d47a1', label='강변공원'),
]
ax.legend(handles=legend_elements, loc='lower left', fontsize=13,
          framealpha=0.9, prop={'family': font_name, 'size': 13})

plt.tight_layout()
out2 = os.path.join(BASE, '지도3_공원경계.png')
plt.savefig(out2, dpi=180, bbox_inches='tight', facecolor='white')
plt.close()
print(f"  저장: {out2}")


# ═══════════════════════════════════════════════════════════════
# 지도 3: 스트라바 세그먼트 polyline
# ═══════════════════════════════════════════════════════════════
print("\n[지도 3] 스트라바 세그먼트 시각화 시작...")

df_strava = pd.read_csv(os.path.join(BASE, '진옥제외_strava_데이터.csv'), encoding='utf-8-sig')
print(f"  스트라바 데이터: {df_strava.shape}")
print(f"  컬럼: {df_strava.columns.tolist()}")

# polyline 디코딩
from shapely.geometry import LineString
import polyline as pl

lines = []
for idx, row in df_strava.iterrows():
    try:
        coords = pl.decode(row['polyline'])  # [(lat, lon), ...]
        if len(coords) >= 2:
            line = LineString([(lon, lat) for lat, lon in coords])
            lines.append({'segment_id': row['segment_id'], 'geometry': line})
    except Exception as e:
        pass

print(f"  디코딩 성공: {len(lines)}개 세그먼트")

gdf_strava = gpd.GeoDataFrame(lines, geometry='geometry', crs='EPSG:4326')

fig, ax = plt.subplots(1, 1, figsize=(14, 12))
ax.set_facecolor('white')
fig.patch.set_facecolor('white')

# 서울 경계
seoul_wgs.plot(ax=ax, color='#f5f5f5', edgecolor='#aaaaaa', linewidth=1.0, zorder=1)

# 스트라바 세그먼트
gdf_strava.plot(
    ax=ax,
    color='#ff7f00',
    linewidth=3.0,
    alpha=0.85,
    zorder=2
)

# 서울 경계선 위에 덧그리기
seoul_wgs.plot(ax=ax, color='none', edgecolor='#555555', linewidth=1.2, zorder=3)

ax.set_xlim(xmin - margin, xmax + margin)
ax.set_ylim(ymin - margin, ymax + margin)
ax.set_title('서울시 스트라바 세그먼트', fontsize=18, fontweight='bold',
             color='black', pad=15, fontname=font_name)

# 세그먼트 수 텍스트
ax.text(0.02, 0.03, f'총 {len(lines):,}개 세그먼트',
        transform=ax.transAxes, fontsize=12, color='#333333',
        fontname=font_name, bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8))

ax.set_axis_off()

plt.tight_layout()
out3 = os.path.join(BASE, '지도4_스트라바세그먼트.png')
plt.savefig(out3, dpi=180, bbox_inches='tight', facecolor='white')
plt.close()
print(f"  저장: {out3}")

print("\n모든 시각화 완료!")
print(f"  - {out1}")
print(f"  - {out2}")
print(f"  - {out3}")
