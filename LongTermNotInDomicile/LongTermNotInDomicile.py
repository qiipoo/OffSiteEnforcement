
import os
import glob
import platform
import sys
import numpy as np
import pandas as pd
import geopandas as gpd
from datetime import datetime, timedelta
import sqlalchemy
from mysql import connector
from urllib import parse

mysql_properties = {
    'host': '10.212.160.143',
    'user': 'lkyw',
    'password': 'lkyw@123',
    'database': 'lkyw',
    'buffered': True,
    'pool_size': 10,
}


VEHICLE_PLATE_COLOR = {
    'A': '白色',
    'B': '灰色',
    'C': '黄色',
    'D': '粉色',
    'E': '红色',
    'F': '紫色',
    'G': '绿色',
    'H': '蓝色',
    'I': '棕色',
    'J': '黑色',
    'K': '渐变绿色',
    'L': '黄绿色',
    'Z': '其它色',
    'YB': '银白色',
    'RW': '红/白色',
    'BW': '黄/白色',
    'OW': '橙/白色'
}

YEHU_AREA = {
    'DCQ': '东城区',
    'XCQ': '西城区',
    'CYQ': '朝阳区',
    'FTQ': '丰台区',
    'SJSQ': '石景山区',
    'HDQ': '海淀区',
    'MTGQ': '门头沟区',
    'FSQ': '房山区',
    'TZQ': '通州区',
    'SYQ': '顺义区',
    'CPQ': '昌平区',
    'DXQ': '大兴区',
    'HRQ': '怀柔区',
    'PGQ': '平谷区',
    'MYQ': '密云区',
    'YQQ': '延庆区',
    'YS': '燕山',
    'KFQ': '开发区',
    'JJJSKFQ': '经济技术开发区',
    'BJJJJSKFQ': '北京经济技术开发区',
    'BJS': '北京市',
}

if __name__ == '__main__':

    output_file = 'sj_long_term_not_in_registered_area'

    if len(sys.argv) > 1:
        day_range = int(sys.argv[1])
    else:
        day_range = 14
    sys_str = platform.system()
    if sys_str == 'Windows':
        input_gps_path = 'C:/Users/JT-0919/Downloads'
        output_path = 'C:/Users/JT-0919/Downloads'
        output_history_path = 'C:/Users/JT-0919/Downloads'
        geo_json = 'file/beijing.geoJson'
    else:
        input_gps_path = '/data/ActiveMQ/ky_file'
        output_path = '/root/data'
        output_history_path = '/root/data/history'
        geo_json = '/root/program/file/beijing.geoJson'

    today = datetime.today().date()
    yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')
    last_day = (datetime.today() - timedelta(days=day_range)).strftime('%Y%m%d')

    sql_yehu = 'SELECT ID YEHU_ID, NAME, CONTACT_NAME, CONTACT_PHONE, CONTACT_MOBILE, AREA, CERT_AUTH_NAME ' \
               'FROM KAAD21_PASSENGER_YEHU'
    sql_vehicle = 'SELECT CERTIFICATE_NO, VEHICLE_NO, PLATE_COLOR, YEHU_ID, GPS_DEVICE_MODEL, GPS_PLATFROM_NAME, ' \
                  'GRADING_DATE, GRADING_DEADLINE, VEHICLE_BIZ, PASSENGER_LINE_ID ' \
                  'FROM KAAD19_PASSENGER_VEHICLE WHERE VECHILE_STATUS = "YY"'
    sql_line = 'SELECT ID PASSENGER_LINE_ID, LINE_NAME FROM KAAD16_PASSENGER_LINE'
    mysql_conn = connector.connect(**mysql_properties)
    mysql_engine = sqlalchemy.create_engine(
        'mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset=utf8mb4'.format(
            user='lkyw',
            password=parse.quote_plus('lkyw@123'),
            host='10.212.160.143',
            port=3306,
            db='lkyw'
        ))

    data_yehu = pd.read_sql(sql_yehu, con=mysql_engine.connect())
    data_vehicle = pd.read_sql(sql_vehicle, con=mysql_engine.connect())
    data_line = pd.read_sql(sql_line, con=mysql_engine.connect())

    data_yehu['AREA'] = data_yehu['AREA'].apply(lambda x: YEHU_AREA.get(x))
    data_vehicle['PLATE_COLOR'] = data_vehicle['PLATE_COLOR'].apply(lambda x: VEHICLE_PLATE_COLOR.get(x))

    approval_data = pd.merge(data_vehicle, data_yehu, on='YEHU_ID', how='left')
    approval_data = pd.merge(approval_data, data_line, on='PASSENGER_LINE_ID', how='left')
    vehicle_no = approval_data['VEHICLE_NO'].tolist()

    beijing_gdf = gpd.read_file(geo_json)
    beijing_polygon = beijing_gdf.unary_union

    gps_files = glob.glob(os.path.join(input_gps_path, '*.csv'))
    gps_days = 0
    input_data_lst = []
    for idx, in_file in enumerate(gps_files):
        file_name = in_file.split('/')[-1]
        if file_name.find('GPS_') > -1 or file_name.find('KY_GPS_') > -1:
            if os.path.getsize(in_file) == 0:
                print('Zero size file: %s' % in_file)
                continue
            day = file_name.split('_')[-1].split('.')[0]
            if last_day <= day < today.strftime('%Y%m%d'):
                gps_days = gps_days + 1
                in_data = pd.read_csv(in_file, encoding="utf-8", keep_default_na=False)
                in_data = in_data[['车牌号', '时间', '经度', '纬度']]
                in_data.dropna(subset=['时间'], inplace=True)
                in_data.dropna(subset=['经度', '纬度'], inplace=True)
                in_data = in_data[in_data['车牌号'].isin(vehicle_no)]
                in_data['gps日期'] = in_data['时间'].apply(lambda x: str.replace(x[:10], '-', ''))
                in_data = in_data.query('gps日期 == @day')
                in_data.drop(columns=['gps日期'], inplace=True)
                in_data = in_data.query('经度 > 73 & 经度 < 136')
                in_data = in_data.query('纬度 > 3 & 纬度 < 54')
                input_data_lst.append(in_data)
    if len(input_data_lst) > 1:
        gps_data = pd.concat(input_data_lst, ignore_index=True)
    elif len(input_data_lst) == 1:
        gps_data = input_data_lst[0]
    else:
        print('Not found gps data')
        exit(1)

    gdf = gpd.GeoDataFrame(gps_data, geometry=gpd.points_from_xy(gps_data.经度, gps_data.纬度), crs="EPSG:4326")
    within_beijing = gdf.within(beijing_polygon)
    gps_data['within_beijing'] = within_beijing

    # 将日期及时间字段转换为日期格式
    gps_data['时间'] = pd.to_datetime(gps_data['时间'], errors='coerce')

    # 只取日期部分
    gps_data['日期'] = gps_data['时间'].dt.date

    # 找到数据集中的最早时间和最新时间
    start_date = gps_data['日期'].min()
    end_date = gps_data['日期'].max()

    # 为每一辆车生成一个完整的日期序列, 并和原始数据进行合并
    df_full_dates = pd.DataFrame()
    for license_plate in gps_data['车牌号'].unique():
        df_dates = pd.date_range(start_date, end_date).to_frame(index=False, name='日期')
        df_dates['日期'] = pd.to_datetime(df_dates['日期']).dt.date
        df_dates['车牌号'] = license_plate
        df_full_dates = pd.concat([df_full_dates, df_dates])

    # 合并完整的日期序列和原始数据
    df_merged = pd.merge(df_full_dates, gps_data, on=['车牌号', '日期'], how='left')

    # 从这里可以替换下边注释掉的代码
    # 创建一个新的列“有GPS数据”, 对于有GPS数据的日期, 设置为True, 对于没有GPS数据的日期, 设置为False
    df_merged['有GPS数据'] = df_merged['经度'].notna()

    # 对于没有数据的日期, 我们假设车辆不在北京
    df_merged['within_beijing'].fillna(False, inplace=True)

    # 按车牌号和日期分组, 如果一整天within_beijing都为False, 则标记为1, 否则为0
    df_grouped = df_merged.groupby(['车牌号', '日期'], as_index=False).agg({'within_beijing': lambda x: 0 if x.any() else 1, '有GPS数据': 'any'})

    def calculate_consecutive_days(group):
        group = group[(group['有GPS数据']) & (group['within_beijing'] == 1)]  # 只考虑那些有GPS数据且在车籍地的日期
        group.sort_values('日期', inplace=True)
        group['日期'] = pd.to_datetime(group['日期'])
        group['日期差'] = group['日期'].diff().dt.days
        group['连续组'] = (group['日期差'] != 1).cumsum()
        group['连续天数'] = group.groupby(['车牌号', '连续组'])['日期'].transform('count')
        group['最大连续天数开始日期'] = group.groupby(['车牌号', '连续组'])['日期'].transform('first')
        group['最大连续天数结束日期'] = group.groupby(['车牌号', '连续组'])['日期'].transform('last')
        return group

    # 按车牌号排序并计算连续的天数
    df_grouped = df_grouped.groupby('车牌号').apply(calculate_consecutive_days).reset_index(drop=True)

    # 对于没有连续不在北京的车辆或最近7天都在北京的车辆, 可能没有相应的记录, 因此在执行以下操作之前, 我们需要先进行检查。
    if not df_grouped.empty:
        max_days_out_group = df_grouped.loc[df_grouped.groupby('车牌号')['连续天数'].idxmax()]

        # # 计算连续不在北京的最大天数
        # max_days_out = df_grouped.groupby('车牌号')['连续天数'].max()
        #
        # # 获取整个数据集的最后一天
        # last_date = df_grouped['日期'].max()
        #
        # # 计算最近7天连续不在北京的最大天数
        # last_7_days = df_grouped[
        #     (df_grouped['日期'] > last_date - pd.Timedelta(days=7)) & (df_grouped['日期'] <= last_date)]
        # last_7_days = last_7_days.groupby('车牌号').apply(calculate_consecutive_days).reset_index(drop=True)
        # max_days_out_last_7_days = last_7_days.groupby('车牌号')['连续天数'].max()

        # 输出结果
        result = pd.DataFrame({
            '车牌号': df_grouped['车牌号'].unique(),
            '不包括全天没有卫星定位的连续不在车籍地的最大天数': max_days_out_group.set_index('车牌号')['连续天数'],
            '不包括全天没有卫星定位的最大连续天数开始日期': max_days_out_group.set_index('车牌号')['最大连续天数开始日期'],
            '不包括全天没有卫星定位的最大连续天数结束日期': max_days_out_group.set_index('车牌号')['最大连续天数结束日期'],
        })
        result = result.fillna(0) # 填充缺失值为0, 对于那些没有连续不在北京的车辆或最近7天都在北京的车辆
    else:
        result = pd.DataFrame(columns=['车牌号', '不包括全天没有卫星定位的连续不在车籍地的最大天数', '不包括全天没有卫星定位的最大连续天数开始日期', '不包括全天没有卫星定位的最大连续天数结束日期'])

    result = result.rename(columns={'车牌号': 'VEHICLE_NO'}).reset_index()
    output_data = pd.merge(result, approval_data, on='VEHICLE_NO', how='left')

    output_data = output_data[['车牌号', '不包括全天没有卫星定位的连续不在车籍地的最大天数',
                               '不包括全天没有卫星定位的最大连续天数开始日期', '不包括全天没有卫星定位的最大连续天数结束日期',
                               'VEHICLE_BIZ', 'LINE_NAME',
                               'NAME', 'CONTACT_NAME', 'CONTACT_PHONE', 'CONTACT_MOBILE', 'AREA', 'CERT_AUTH_NAME']]

    output_data.rename(columns={'车牌号': 'vehicle_no',
                                '不包括全天没有卫星定位的连续不在车籍地的最大天数': 'days',
                                '不包括全天没有卫星定位的最大连续天数开始日期': 'start_date',
                                '不包括全天没有卫星定位的最大连续天数结束日期': 'end_date',
                                'VEHICLE_BIZ': 'business_scope', 'LINE_NAME': 'origin_destination',
                                'NAME': 'company_name', 'CONTACT_NAME': 'contact', 'CONTACT_PHONE': 'tel',
                                'CONTACT_MOBILE': 'phone', 'AREA': 'registered_area',
                                'CERT_AUTH_NAME': 'issuing_auth'}, inplace=True)

    output_data['start_date'] = output_data['start_date'].apply(lambda x: x.strftime('%Y%m%d'))
    output_data['end_date'] = output_data['end_date'].apply(lambda x: x.strftime('%Y%m%d'))

    output_data.sort_values(['days', 'company_name'], inplace=True)
    if gps_days < 7:
        output_data = output_data.query('days == @gps_days')
    else:
        output_data = output_data.query('days >= 7')

    output_data.to_csv(os.path.join(output_history_path, output_file + '_' + today.strftime('%Y%m%d') + '.csv'), index=False)
    output_data.to_csv(os.path.join(output_path, output_file + '.csv'), index=False)
    print('Execute complete')
