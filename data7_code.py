"""
VIIRS Nighttime Lights Data Download Script
下载费城地区的夜间灯光数据
"""

import earthaccess
import os
from datetime import datetime
import geopandas as gpd
from shapely.geometry import box

# ========================================
# 第一部分：设置参数
# ========================================

# 费城的边界框 (左下角经纬度, 右上角经纬度)
PHILLY_BBOX = (-75.280303, 39.867004, -74.955763, 40.137992)

# 时间范围 (建议下载最近一年的月度合成数据)
START_DATE = "2023-01-01"
END_DATE = "2024-10-31"

# 输出目录
OUTPUT_DIR = "viirs_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("=" * 60)
print("VIIRS夜间灯光数据下载脚本")
print("=" * 60)

# ========================================
# 第二部分：登录NASA Earthdata
# ========================================

print("\n步骤1: 登录NASA Earthdata...")
print("请输入你的NASA Earthdata账号信息：")

# 方法1：直接输入（每次都要输）
auth = earthaccess.login()

# 方法2：保存登录信息（只需要登录一次）
# auth = earthaccess.login(strategy="interactive", persist=True)

if auth:
    print("✓ 登录成功！")
else:
    print("✗ 登录失败，请检查账号密码")
    exit()

# ========================================
# 第三部分：搜索数据
# ========================================

print("\n步骤2: 搜索费城地区的VIIRS数据...")

# 搜索VNP46A2产品（月度合成数据，推荐用这个）
results = earthaccess.search_data(
    short_name='VNP46A2',  # 月度合成产品
    cloud_hosted=True,
    bounding_box=PHILLY_BBOX,
    temporal=(START_DATE, END_DATE),
)

print(f"✓ 找到 {len(results)} 个数据文件")

if len(results) == 0:
    print("没有找到数据，请检查时间范围和边界框")
    exit()

# 显示前5个结果
print("\n前5个数据文件：")
for i, granule in enumerate(results[:5]):
    print(f"  {i+1}. {granule['umm']['TemporalExtent']}")

# ========================================
# 第四部分：下载数据
# ========================================

print("\n步骤3: 下载数据...")
print(f"将下载到: {OUTPUT_DIR}/")
print("(这可能需要几分钟，取决于网速)")

# 下载所有找到的文件
downloaded_files = earthaccess.download(
    results,
    local_path=OUTPUT_DIR,
)

print(f"\n✓ 成功下载 {len(downloaded_files)} 个文件！")
print(f"文件保存在: {os.path.abspath(OUTPUT_DIR)}")

# ========================================
# 第五部分：提取和处理数据
# ========================================

print("\n步骤4: 提取夜间灯光亮度值...")

import rasterio
from rasterio.mask import mask
import numpy as np
import pandas as pd

# 创建费城边界的多边形
philly_geom = box(*PHILLY_BBOX)

processed_data = []

for file_path in downloaded_files:
    if file_path.endswith('.h5'):  # VIIRS数据是HDF5格式
        print(f"处理: {os.path.basename(file_path)}")
        
        try:
            # 打开HDF5文件（VIIRS数据的特定波段）
            with rasterio.open(f'HDF5:{file_path}://HDFEOS/GRIDS/VNP_Grid_DNB/Data_Fields/DNB_BRDF-Corrected_NTL') as src:
                
                # 裁剪到费城范围
                out_image, out_transform = mask(src, [philly_geom], crop=True)
                
                # 计算平均亮度
                valid_data = out_image[out_image > 0]  # 排除无效值
                if len(valid_data) > 0:
                    mean_brightness = np.mean(valid_data)
                    median_brightness = np.median(valid_data)
                    
                    # 从文件名提取日期
                    filename = os.path.basename(file_path)
                    date_str = filename.split('.')[1][1:]  # 提取日期部分
                    
                    processed_data.append({
                        'date': date_str,
                        'mean_brightness': mean_brightness,
                        'median_brightness': median_brightness,
                        'file': filename
                    })
                    
                    print(f"  ✓ 平均亮度: {mean_brightness:.2f}")
        
        except Exception as e:
            print(f"  ✗ 处理失败: {e}")
            continue

# 保存处理结果
if processed_data:
    df_viirs = pd.DataFrame(processed_data)
    output_csv = os.path.join(OUTPUT_DIR, 'viirs_philadelphia_summary.csv')
    df_viirs.to_csv(output_csv, index=False)
    print(f"\n✓ 汇总数据已保存: {output_csv}")
    print("\n数据预览：")
    print(df_viirs.head())
else:
    print("\n⚠ 没有成功处理任何数据")

# ========================================
# 第六部分：创建简化版本（如果上面太慢）
# ========================================

print("\n" + "=" * 60)
print("备选方案：如果下载太慢或失败")
print("=" * 60)
print("""
你也可以使用Google Earth Engine (GEE) 来获取VIIRS数据：

1. 注册GEE账号: https://earthengine.google.com/signup/
2. 使用以下Python代码（需要先安装: pip install earthengine-api）:

import ee
ee.Initialize()

# 定义费城范围
philly = ee.Geometry.Rectangle([-75.280303, 39.867004, -74.955763, 40.137992])

# 获取VIIRS数据
viirs = ee.ImageCollection('NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG') \\
    .filterDate('2023-01-01', '2024-10-31') \\
    .filterBounds(philly) \\
    .select('avg_rad')

# 计算平均值
mean_brightness = viirs.mean().reduceRegion(
    reducer=ee.Reducer.mean(),
    geometry=philly,
    scale=500
).getInfo()

print(f"费城平均夜间亮度: {mean_brightness}")

这个方法更简单快速！
""")

print("\n" + "=" * 60)
print("下载完成！")
print("=" * 60)
