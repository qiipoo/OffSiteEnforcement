# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import math
import copy
import time
import datetime
import re
import os
import sys

# 文件定义
input_path = r'C:\Users\JT-0919\Desktop\WORK\执法项目\09.数据\20221020'
input_file = r'C:\Users\JT-0919\Desktop\WORK\执法项目\09.数据\20221020\系统导出数据总表v3.xlsx'

output_path = r'C:\Users\JT-0919\Desktop\WORK\执法项目\09.数据\OUTPUT'
output_total_file = r'网约车汇总表'
output_detail_file = r'网约车明细表'
output_duplicate_file = r'网约车重复明细表'
file_type_xlsx = '.xlsx'
file_type_csv = '.csv'

# EXCEL or CSV
output_type = 'EXCEL'

# 文件读取
input_data = pd.read_excel(input_file, na_filter=False)


# 统计操作
output_total_data = {}
output_detail_data = {}
duplicate_data = []
output_up_loc_cnt = {}
output_down_loc_cnt = {}

for idx, itr in enumerate(input_data.itertuples(), start=0):

    ord = itr[1]
    plt = itr[2]
    ch = itr[3]
    nm = itr[4]
    uid = itr[5]
    up_x = itr[6]
    up_y = itr[7]
    down_x = itr[8]
    down_y = itr[9]
    up_dt = itr[10]
    down_dt = itr[11]
    up_loc = itr[12]
    down_loc = itr[13]

    if isinstance(ch, float) or ch == '':
        continue

    data_key = ch + '-' + str(up_dt) + '-' + str(down_dt)
    print(data_key)
    data_value = output_detail_data.get(data_key)
    if data_value is None:
        one_data = {
            '订单号': ord,
            '平台公司': plt,
            '车牌号': ch,
            '姓名': nm,
            '身份证号': uid,
            '上车经度': up_x,
            '上车纬度': up_y,
            '下车经度': down_x,
            '下车纬度': down_y,
            '上车时间': up_dt,
            '下车时间': down_dt,
            '上车地点': up_loc,
            '下车地点': down_loc
        }
        output_detail_data.update({data_key: one_data})

        # 统计服务区和检查站
        up_loc_fwq = output_total_data.get('up_loc_fwq')
        up_loc_jcz = output_total_data.get('up_loc_jcz')
        up_loc_jyz = output_total_data.get('up_loc_jyz')

        down_loc_fwq = output_total_data.get('down_loc_fwq')
        down_loc_jcz = output_total_data.get('down_loc_jcz')
        down_loc_jyz = output_total_data.get('down_loc_jyz')

        # 上车地点
        if re.search(r'服务区', up_loc):
            if up_loc_fwq is None:
                up_loc_fwq = 1
            else:
                up_loc_fwq = up_loc_fwq + 1

        if re.search(r'检查站', up_loc):
            if up_loc_jcz is None:
                up_loc_jcz = 1
            else:
                up_loc_jcz = up_loc_jcz + 1

        if re.search(r'检验站', up_loc):
            if up_loc_jyz is None:
                up_loc_jyz = 1
            else:
                up_loc_jyz = up_loc_jyz + 1

        # 下车地点
        if re.search(r'服务区', down_loc):
            if down_loc_fwq is None:
                down_loc_fwq = 1
            else:
                down_loc_fwq = down_loc_fwq + 1

        if re.search(r'检查站', down_loc):
            if down_loc_jcz is None:
                down_loc_jcz = 1
            else:
                down_loc_jcz = down_loc_jcz + 1

        if re.search(r'检验站', down_loc):
            if down_loc_jyz is None:
                down_loc_jyz = 1
            else:
                down_loc_jyz = down_loc_jyz + 1

        one_total = {
            'up_loc_jcz': up_loc_jcz,
            'up_loc_fwq': up_loc_fwq,
            'up_loc_jyz': up_loc_jyz,
            'down_loc_jcz': down_loc_jcz,
            'down_loc_fwq': down_loc_fwq,
            'down_loc_jyz': down_loc_jyz
        }

        output_total_data.update(one_total)

        up_loc_key = str.replace(up_loc, '-', '')
        up_loc_value = output_up_loc_cnt.get(up_loc_key)
        if up_loc_value is None:
            up_loc_cnt = 1
        else:
            up_loc_cnt = up_loc_value + 1
        output_up_loc_cnt.update({up_loc_key: up_loc_cnt})

        down_loc_key = str.replace(down_loc, '-', '')
        down_loc_value = output_down_loc_cnt.get(down_loc_key)
        if down_loc_value is None:
            down_loc_cnt = 1
        else:
            down_loc_cnt = down_loc_value + 1
        output_down_loc_cnt.update({down_loc_key: down_loc_cnt})

        if isinstance(up_dt, str):
            up_tm = str.split(up_dt, ' ')[1]

        if isinstance(down_dt, str):
            down_tm = str.split(down_dt, ' ')[1]

    else:
        dup_data = {
            '订单号': ord,
            '平台公司': plt,
            '车牌号': ch,
            '姓名': nm,
            '身份证号': uid,
            '上车经度': up_x,
            '上车纬度': up_y,
            '下车经度': down_x,
            '下车纬度': down_y,
            '上车时间': up_dt,
            '下车时间': down_dt,
            '上车地点': up_loc,
            '下车地点': down_loc
        }
        duplicate_data.append(data_value)
        duplicate_data.append(dup_data)


# 排序
sorted_duplicate_data = sorted(duplicate_data, key=lambda x: x['车牌号'], reverse=True)
sorted_output_up_loc_cnt = sorted(output_up_loc_cnt.items(), key=lambda x: x[1], reverse=True)
sorted_output_down_loc_cnt = sorted(output_down_loc_cnt.items(), key=lambda x: x[1], reverse=True)


# 服务区检查站统计输出
up_loc_fwq = output_total_data.get('up_loc_fwq')
up_loc_jcz = output_total_data.get('up_loc_jcz')
up_loc_jyz = output_total_data.get('up_loc_jyz')

down_loc_fwq = output_total_data.get('down_loc_fwq')
down_loc_jcz = output_total_data.get('down_loc_jcz')
down_loc_jyz = output_total_data.get('down_loc_jyz')

print('【上车地点统计】 服务区:%d, 检查站:%d, 检验站:%d, 合计:%d'
      % (up_loc_fwq, up_loc_jcz, up_loc_jyz, up_loc_fwq + up_loc_jcz + up_loc_jyz))
print('【下车地点统计】 服务区:%d, 检查站:%d, 检验站:%d, 合计:%d'
      % (down_loc_fwq, down_loc_jcz, down_loc_jyz, down_loc_fwq + down_loc_jcz + down_loc_jyz))

# 上下车地点排名输出
lst_up_loc = []
lst_up_loc_cnt = []
for a_up_loc_cnt in sorted_output_up_loc_cnt:
    lst_up_loc.append(a_up_loc_cnt[0])
    lst_up_loc_cnt.append(a_up_loc_cnt[1])

lst_down_loc = []
lst_down_loc_cnt = []
for a_down_loc_cnt in sorted_output_down_loc_cnt:
    lst_down_loc.append(a_down_loc_cnt[0])
    lst_down_loc_cnt.append(a_down_loc_cnt[1])

df_up_loc_cnt = pd.DataFrame.from_dict({'上车地点': lst_up_loc, '次数': lst_up_loc_cnt})
df_down_loc_cnt = pd.DataFrame.from_dict({'下车地点': lst_down_loc, '次数': lst_down_loc_cnt})

total_excel_file = os.path.join(output_path, output_total_file + file_type_xlsx)
if os.path.exists(total_excel_file):
    os.remove(total_excel_file)

with pd.ExcelWriter(total_excel_file, engine='openpyxl') as writer:
    df_up_loc_cnt.to_excel(writer, sheet_name='上车地点统计', index=False)

with pd.ExcelWriter(total_excel_file, mode='a', engine='openpyxl') as writer:
    df_down_loc_cnt.to_excel(writer, sheet_name='下车地点统计', index=False)

# 明细和重复输出
if output_type == 'EXCEL':
    # 明细输出
    detail_excel_file = os.path.join(output_path, output_detail_file + file_type_xlsx)
    df_out_value = pd.DataFrame.from_dict(list(output_detail_data.values()))
    with pd.ExcelWriter(detail_excel_file, engine='openpyxl') as writer:
        df_out_value.to_excel(writer, sheet_name='网约车明细', index=False)

    # 重复输出
    duplicate_excel_file = os.path.join(output_path, output_duplicate_file + file_type_xlsx)
    # df_out_value = pd.DataFrame(sorted_duplicate_data)
    df_out_value = pd.DataFrame.from_dict(sorted_duplicate_data)
    with pd.ExcelWriter(duplicate_excel_file, engine='openpyxl') as writer:
        df_out_value.to_excel(writer, sheet_name='网约车重复明细', index=False)

elif output_type == 'CSV':
    csv_file_title = ('订单号', '平台公司', '车牌号', '姓名', '身份证号',
                      '上车经度', '上车纬度', '下车经度', '下车纬度',
                      '上车时间', '下车时间', '上车地点', '下车地点')

    # 明细输出
    detail_csv_file = os.path.join(output_path, output_detail_file + file_type_csv)
    with open(detail_csv_file, 'w') as f:
        f.write('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % csv_file_title)

        for idx, a_key in enumerate(output_detail_data.keys()):
            a_data = output_detail_data.get(a_key)
            lst_value = []
            for a_value in a_data.values():
                lst_value.append(str(a_value))
            f.write(','.join(lst_value))
            f.write('\n')

    # 重复输出
    duplicate_csv_file = os.path.join(output_path, output_duplicate_file + file_type_csv)
    with open(duplicate_csv_file, 'w') as f:
        f.write('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % csv_file_title)

        for a_data in sorted_duplicate_data:
            lst_value = []
            for a_value in a_data.values():
                lst_value.append(str(a_value))
            f.write(','.join(lst_value))
            f.write('\n')

else:
    print('Unknown file type!')
    pass
