# -*- coding: utf-8 -*-
import os.path
import platform
import numpy as np
import pandas as pd
import time
import json
import requests
from datetime import datetime, timedelta
import glob
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

rktx_title = ['通行标识ID','通行介质类型','OBU序号编码','通行介质编码','入口日期及时间','入口实际车牌号','入口识别车牌号','入口车型代码','入口车种代码','入口车货总重','入口轴数','入口站编号','入口站名称','入口车道编号','入口车道类型']
mjtx_title = ['通行标识ID','门架编号','门架HEX字符串','行驶方向','门架顺序号','门架类型','对向门架Hex值','通过时间','通行介质类型','OBU序号编码','通行介质编码','计费车辆车牌号','识别车辆车牌号','计费车型代码','计费车种代码','车轴数','车辆座位数/载重','车道编号','车辆速度','入口站hex字符串','入口名称','入口日期及时间','入口状态','入口车货总重','上一个门架的HEX字符串','通过上一个门架的时间']
cktx_title = ['通行标识ID','通行介质类型','计费方式','OBU序号编码','通行介质编码','出口日期及时间','出口实际车辆车牌号','出口识别车辆车牌号','出口计费车型代码','出口车种代码','出口车货总重','出口数轴','出口站编号','出口站名称','出口车道的编号','出口车道类型','入口编号','入口名称','入口时间','入口车货总重','是否开放式收费站','通行省份个数']

if __name__ == '__main__':

    output_file = 'sj_rating_overdue'

    sys_str = platform.system()
    if sys_str == 'Windows':
        input_gps_path = 'C:/Users/JT-0919/Downloads'
        output_path = 'C:/Users/JT-0919/Downloads'
        output_history_path = 'C:/Users/JT-0919/Downloads'
    else:
        input_gps_path = '/data/ActiveMQ/ky_file'
        output_path = '/root/data'
        output_history_path = '/root/data/history'

    today = datetime.today().date()
    yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')

    sql_yehu = 'SELECT ID YEHU_ID, NAME, CONTACT_NAME, CONTACT_PHONE, CONTACT_MOBILE, AREA, CERT_AUTH_NAME ' \
               'FROM KAAD21_PASSENGER_YEHU'
    sql_vehicle = 'SELECT CERTIFICATE_NO, VEHICLE_NO, PLATE_COLOR, YEHU_ID, GPS_DEVICE_MODEL, GPS_PLATFROM_NAME, ' \
                  'GRADING_DATE, GRADING_DEADLINE, VEHICLE_BIZ, PASSENGER_LINE_ID ' \
                  'FROM KAAD19_PASSENGER_VEHICLE WHERE VECHILE_STATUS = "YY"'
    sql_bus = 'SELECT ID BUS_ID, DATE_FORMAT(DRVDATE, "%Y%m%d") DRVDATE, IFNULL(REALBUSLICENSE, BUSLICENSE) VEHICLE_NO ' \
              'FROM LONGDISTANCE_BUS ' \
              'WHERE REALBUSLICENSE <> "null" AND BUSLICENSE <> "null" ' \
              'AND DRVDATE = DATE_FORMAT(DATE_SUB(SYSDATE(), INTERVAL 1 DAY), "%Y-%m-%d")'

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
    data_bus = pd.read_sql(sql_bus, con=mysql_conn)

    data_yehu['AREA'] = data_yehu['AREA'].apply(lambda x: YEHU_AREA.get(x))
    data_vehicle['PLATE_COLOR'] = data_vehicle['PLATE_COLOR'].apply(lambda x: VEHICLE_PLATE_COLOR.get(x))
    data_bus['VEHICLE_NO'] = data_bus['VEHICLE_NO'].apply(lambda x: x.replace('-', ''))

    data_vehicle = data_vehicle.query('GRADING_DEADLINE < @today')
    data_vehicle['GRADING_EXPIRE_DAYS'] = (today - data_vehicle['GRADING_DEADLINE']).dt.days

    output_data = pd.merge(data_vehicle, data_bus, on='VEHICLE_NO', how='inner')
    output_data = output_data.merge(data_yehu, on='YEHU_ID', how='left')

    data_gps = pd.DataFrame(data=None)
    gps_files = glob.glob(os.path.join(input_gps_path, '*.csv'))
    for idx, in_file in enumerate(gps_files):
        file_name = in_file.split('/')[-1]
        if file_name.find('GPS_') > -1 or file_name.find('KY_GPS_') > -1:
            if os.path.getsize(in_file) == 0:
                print('Zero size file: %s' % in_file)
                continue
            gps_date = file_name.split('_')[-1].split('.')[0]
            if yesterday == gps_date:
                data_gps = pd.read_csv(in_file, keep_default_na=False)

    if not data_gps.empty:
        data_gps_filter = data_gps[data_gps['车牌号'].isin(output_data['VEHICLE_NO'].tolist())]
        output_data = output_data[-output_data['VEHICLE_NO'].isin(data_gps_filter['车牌号'].tolist())]

    output_data['GRADING_DEADLINE'] = output_data['GRADING_DEADLINE'].astype('string')
    output_data['GRADING_DEADLINE'] = output_data['GRADING_DEADLINE'].apply(lambda x: x.replace('-', ''))

    output_data = output_data[['CERTIFICATE_NO', 'VEHICLE_NO', 'PLATE_COLOR', 'NAME', 'CONTACT_NAME', 'CONTACT_PHONE',
                               'CONTACT_MOBILE', 'AREA', 'GRADING_EXPIRE_DAYS', 'GRADING_DEADLINE', 'DRVDATE']]

    output_data.rename(columns={'CERTIFICATE_NO': 'transport_certificate', 'VEHICLE_NO': 'vehicle_no',
                                'PLATE_COLOR': 'plate_color', 'NAME': 'company_name',
                                'CONTACT_NAME': 'contact', 'CONTACT_PHONE': 'tel', 'CONTACT_MOBILE': 'phone',
                                'AREA': 'registered_area', 'GRADING_EXPIRE_DAYS': 'rating_overdue_days',
                                'GRADING_DEADLINE': 'rating_deadline', 'DRVDATE': 'departure_date'}, inplace=True)

    etc_vehicle = []
    try:
        payload = {'date': yesterday, 'vehicleno': output_data['vehicle_no'].tolist()}
        etc_url = 'http://10.212.138.156:8085/etclkyw'
        rep = requests.post(etc_url, data=json.dumps(payload))
        if rep.status_code == 200:
            try:
                rep_json = json.loads(rep.text)
            except json.decoder.JSONDecodeError as e:
                print(rep.text)
                rep_json = {'CKTX': [], 'RKTX': [], 'MJTX': []}

            lst_cktx = []
            lst_rktx = []
            lst_mjtx = []
            for a_data in rep_json['CKTX']:
                lst_cktx.append(a_data.split(','))
            for a_data in rep_json['RKTX']:
                lst_rktx.append(a_data.split(','))
            for a_data in rep_json['MJTX']:
                lst_mjtx.append(a_data.split(','))

            df_cktx = pd.DataFrame(lst_cktx, columns=cktx_title)
            df_rktx = pd.DataFrame(lst_rktx, columns=rktx_title)
            df_mjtx = pd.DataFrame(lst_mjtx, columns=mjtx_title)

            df_cktx['车牌号'] = df_cktx['出口实际车辆车牌号'].apply(lambda x: str.split(x, '_')[0])
            df_rktx['车牌号'] = df_rktx['入口实际车牌号'].apply(lambda x: str.split(x, '_')[0])
            df_mjtx['车牌号'] = df_mjtx['计费车辆车牌号'].apply(lambda x: str.split(x, '_')[0])

            etc_vehicle.extend(df_cktx['车牌号'].tolist())
            etc_vehicle.extend(df_rktx['车牌号'].tolist())
            etc_vehicle.extend(df_mjtx['车牌号'].tolist())

    except Exception as e:
        print(e)

    if len(etc_vehicle) > 0:
        output_data = output_data[output_data['vehicle_no'].isin(etc_vehicle)]
        output_data['etc_date'] = yesterday
    else:
        output_data['etc_date'] = ''
    if not data_gps.empty:
        output_data['gps_date'] = yesterday
    else:
        output_data['gps_date'] = ''

    output_data.to_csv(os.path.join(output_history_path, output_file + '_' + today.strftime('%Y%m%d') + '.csv'), index=False)
    output_data.to_csv(os.path.join(output_path, output_file + '.csv'), index=False)
    print('Execute complete')
