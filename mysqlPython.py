import mysql.connector
import datetime

mysql_properties = {
    'host': '10.212.160.143',
    'user': 'lkyw',
    'password': 'lkyw@123',
    'database': 'lkyw',
    'buffered': True,
    'pool_size': 10,
}

mysql_conn = mysql.connector.connect(**mysql_properties)
mysql_cursor = mysql_conn.cursor()

mysql_cursor.execute("SELECT * FROM KAAD11_FREIGHT_YEHU LIMIT 1")

mysql_column_names = mysql_cursor.column_names

pre_stmt = f'INSERT INTO KAAD11_FREIGHT_YEHU ('
columns = ''
values = ''
for col in mysql_column_names:
    columns = columns + col + ', '
    values = values + '%s, '

print(len(mysql_column_names))

mysql_stmt = pre_stmt + columns[:-2] + ') VALUES (' + values[:-2] + ')'
print(mysql_stmt)

# mysql_result = mysql_cursor.fetchall()
# for a_rec in mysql_result:
#     print(a_rec)

mysql_cursor.execute("TRUNCATE KAAD11_FREIGHT_YEHU")
mysql_conn.commit()


rec = ('9756dccced954f7294d8ccd2fd404e64', '北京宇华佳运航空货运代理有限公司', '59417c975f13471b8eea3cc3c90951c5',
       '货110113011853', '北京市顺义区南法信镇顺畅大道1号', '101300', '王新华', '330323196809285536', '64544132', None,
       '861ca036a2ad4c31bd0bd048e884c2b8', '6d611459bfec4d61a4eebd8796cb5d88', '北京市顺义区交通局', None, None, None,
       '1', None, '1', None, None, None, None, None, None, None, None, None, None, None, '1', None, 'JYX', 'YES',
       None, None, 'RMB', None, datetime.date(2009, 4, 22), datetime.date(2013, 4, 21), 'XY', 'DLHS', 'ZC', None,
       'NO', datetime.date(2020, 12, 30), datetime.date(2020, 12, 30), None, 'SYQ', '普通货运', 'YES', 'NO', '0',
       '0', '0', '陈小梅', None, None, None, None, None, None, None, None, None, None, None, None, None, None,
       None, None, None, '1', None, None, None, None, None, None, None, '0', 'JMSFZ', None, None, None, None,
       None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
       None, None, None, None, None, None, None, None, None, None, None, None, None, datetime.date(2009, 4, 22),
       None, None, None, None, datetime.date(2021, 8, 19), datetime.date(2022, 7, 12))


print(len(rec))

mysql_cursor.execute(mysql_stmt, list(rec))
mysql_conn.commit()
