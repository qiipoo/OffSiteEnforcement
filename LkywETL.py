# -*- coding: utf-8 -*-
import dmPython
import mysql.connector
import datetime
import time

dm_properties = {
    'user': 'ZHANGXL',
    'password': '6Y,LmU7gC290|4%OKbBvhx3kHew1$Z_/',
    'server': '172.26.57.210',
    'port': 5236,
    'autoCommit': False,
}

mysql_properties = {
    'host': '10.212.160.143',
    'user': 'lkyw',
    'password': 'lkyw@123',
    'database': 'lkyw',
    'buffered': True,
    'pool_size': 10,
}

dm_tables = {
    'GXJTW.KAAD21_PASSENGER_YEHU',
    'GXJTW.KAAD19_PASSENGER_VEHICLE',
    'GXJTW.LONGDISTANCE_BUS',
    'GXJTW.LONGDISTANCE_CAR',
    'GXJTW.KAAD16_PASSENGER_LINE',
    'GXJTW.HAZARDOUS_CHEMICAL_ORDER',
    'GXJTW.KS_NUMBER',
    'GXJTW.KAAD09_FREIGHT_VEHICLE',
    'GXJTW.KAAD11_FREIGHT_YEHU',
    'GXSJSJ.ZFXW_ZLZGXX',
}

mysql_tables = {
    'GXJTW.KAAD21_PASSENGER_YEHU': 'KAAD21_PASSENGER_YEHU',
    'GXJTW.KAAD19_PASSENGER_VEHICLE': 'KAAD19_PASSENGER_VEHICLE',
    'GXJTW.LONGDISTANCE_BUS': 'LONGDISTANCE_BUS',
    'GXJTW.LONGDISTANCE_CAR': 'LONGDISTANCE_CAR',
    'GXJTW.KAAD16_PASSENGER_LINE': 'KAAD16_PASSENGER_LINE',
    'GXJTW.HAZARDOUS_CHEMICAL_ORDER': 'HAZARDOUS_CHEMICAL_ORDER',
    'GXJTW.KS_NUMBER': 'KS_NUMBER',
    'GXJTW.KAAD09_FREIGHT_VEHICLE': 'KAAD09_FREIGHT_VEHICLE',
    'GXJTW.KAAD11_FREIGHT_YEHU': 'KAAD11_FREIGHT_YEHU',
    'GXSJSJ.ZFXW_ZLZGXX': 'ZFXW_ZLZGXX',
}

custom_tables = {
    'GXSJSJ.ZFXW_ZLZGXX': 'SELECT T1.AJID, T1.ZLZGLX, T1.ZGKSRQ, T1.ZGJSRQ, T1.ZGFCRQ, T1.ZGFCJG, T1.FCJGBHG, '
                          'T1.CLCS, T1.ZGQK, T1.CJSJ, T1.XGSJ, T2.CLPH, T2.DSRMC FROM GXSJSJ.ZFXW_ZLZGXX T1, '
                          'GXZFZD.PUNISH_CASE_PARTY T2 WHERE T1.AJID = T2.CASE_NUM',
}

def count_dm_table(table_name):
    dm_conn = dmPython.connect(**dm_properties)
    dm_cursor = dm_conn.cursor()
    dm_sql = f'SELECT COUNT(1) FROM {table_name}'
    dm_cursor.execute(dm_sql)
    dm_result = dm_cursor.fetchall()
    dm_cursor.close()
    dm_conn.close()
    return dm_result


def get_all_dm_table(table_name, offset=-1, custom_sql=''):
    dm_conn = dmPython.connect(**dm_properties)
    dm_cursor = dm_conn.cursor()
    if custom_sql == '':
        if offset == -1:
            dm_sql = f'SELECT * FROM {table_name}'
        else:
            dm_sql = f'SELECT * FROM {table_name} LIMIT 1000000 OFFSET {offset}'
    else:
        dm_sql = custom_sql
    dm_cursor.execute(dm_sql)
    dm_result = dm_cursor.fetchall()
    dm_cursor.close()
    dm_conn.close()
    return dm_result


if __name__ == '__main__':

    start_tm = datetime.datetime.now()

    for a_dm_table in dm_tables:
        print('*' * 80)
        print('START INSERT %s %s' % (a_dm_table, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        part_start_tm = datetime.datetime.now()

        dmCount = count_dm_table(a_dm_table)

        cnt = 1
        if dmCount[0][0] > 1000000:
            cnt = int(dmCount[0][0] / 1000000) + 1
        # print(cnt)

        mysql_table = mysql_tables.get(a_dm_table)

        mysql_conn = mysql.connector.connect(**mysql_properties)
        mysql_cursor = mysql_conn.cursor()
        mysql_stmt = f'SELECT * FROM {mysql_table} LIMIT 1'
        # print(mysql_stmt)
        mysql_cursor.execute(mysql_stmt)
        mysql_column_names = mysql_cursor.column_names

        mysql_stmt = f'TRUNCATE {mysql_table}'
        mysql_cursor.execute(mysql_stmt)
        mysql_conn.commit()
        print(f'{mysql_stmt} SUCCESS')

        pre_stmt = f'INSERT IGNORE INTO {mysql_table} ('
        columns = ''
        values = ''
        for col in mysql_column_names:
            columns = columns + col + ', '
            values = values + '%s, '
        mysql_stmt = pre_stmt + columns[:-2] + ') VALUES (' + values[:-2] + ')'
        # print(mysql_stmt)

        for cur in range(cnt):
            custom_table = custom_tables.get(a_dm_table)
            if cnt == 0:
                if custom_table is None:
                    dmResult = get_all_dm_table(a_dm_table)
                else:
                    dmResult = get_all_dm_table(a_dm_table, custom_sql=custom_table)
            else:
                if cur == 0:
                    offset = 0
                else:
                    offset = cur * 1000000 + 1
                if custom_table is None:
                    dmResult = get_all_dm_table(a_dm_table, offset)
                else:
                    dmResult = get_all_dm_table(a_dm_table, offset, custom_table)

            for idx, a_dm_rec in enumerate(dmResult):
                if idx == 0:
                    break

            print(f'[{cur+1}/{cnt}]INSERT {mysql_table} PROCESSING...')
            try:
                mysql_cursor.executemany(mysql_stmt, dmResult)
                mysql_conn.commit()
                print(f'INSERT {mysql_table} SUCCESS')
            except Exception as e:
                print(e)
                mysql_conn.rollback()
                print(f'\033[1;31mINSERT {mysql_table} FAIL\033[0m')
            print('END INSERT %s %s' % (a_dm_table, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        mysql_cursor.close()
        mysql_conn.close()

        part_end_tm = datetime.datetime.now()
        print(f'{mysql_table} PROCESS USED {(part_end_tm - part_start_tm).seconds} SECONDS')

    end_tm = datetime.datetime.now()
    print(f'ETL PROCESS USED {(end_tm - start_tm).seconds} SECONDS')
