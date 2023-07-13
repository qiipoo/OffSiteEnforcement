import json
import requests
import numpy as np
import pandas as pd
import sqlalchemy
from datetime import datetime, timedelta
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

rktx_title = ['通行标识ID','通行介质类型','OBU序号编码','通行介质编码','入口日期及时间','入口实际车牌号','入口识别车牌号','入口车型代码','入口车种代码','入口车货总重','入口轴数','入口站编号','入口站名称','入口车道编号','入口车道类型']
mjtx_title = ['通行标识ID','门架编号','门架HEX字符串','行驶方向','门架顺序号','门架类型','对向门架Hex值','通过时间','通行介质类型','OBU序号编码','通行介质编码','计费车辆车牌号','识别车辆车牌号','计费车型代码','计费车种代码','车轴数','车辆座位数/载重','车道编号','车辆速度','入口站hex字符串','入口名称','入口日期及时间','入口状态','入口车货总重','上一个门架的HEX字符串','通过上一个门架的时间']
cktx_title = ['通行标识ID','通行介质类型','计费方式','OBU序号编码','通行介质编码','出口日期及时间','出口实际车辆车牌号','出口识别车辆车牌号','出口计费车型代码','出口车种代码','出口车货总重','出口数轴','出口站编号','出口站名称','出口车道的编号','出口车道类型','入口编号','入口名称','入口时间','入口车货总重','是否开放式收费站','通行省份个数']


if __name__ == '__main__':

    sql_vehicle = 'SELECT VEHICLE_NO FROM KAAD19_PASSENGER_VEHICLE WHERE VECHILE_STATUS = "YY"'

    mysql_conn = connector.connect(**mysql_properties)
    mysql_engine = sqlalchemy.create_engine(
        'mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset=utf8mb4'.format(
            user='lkyw',
            password=parse.quote_plus('lkyw@123'),
            host='10.212.160.143',
            port=3306,
            db='lkyw'
        ))

    data_vehicle = pd.read_sql(sql_vehicle, con=mysql_engine.connect())

    today = datetime.today().date()
    yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')

    payload = {'date': yesterday, 'vehicleno': data_vehicle.head()['VEHICLE_NO'].tolist()}

    etc_url = 'http://10.212.138.156:8085/etclkyw'

    rep = requests.post(etc_url, data=json.dumps(payload))

    if rep.status_code == 200:
        print(rep.text)
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

        print(df_cktx)
        print(df_rktx)
        print(df_mjtx)

        etc_vehicle = []
        etc_vehicle.extend(df_cktx['车牌号'].tolist())
        etc_vehicle.extend(df_rktx['车牌号'].tolist())
        etc_vehicle.extend(df_mjtx['车牌号'].tolist())

        unique_vehicle = set(etc_vehicle)

        print(unique_vehicle)

