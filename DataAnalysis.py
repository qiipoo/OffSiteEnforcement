# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import math
import copy
import time
import datetime
import re
import glob

# 文件定义
input_file = r'C:\Users\JT-0919\Desktop\WORK\执法项目\数据\增量\外省入北京出 9.13-10.13 客车明细\*.txt'

output_total_file = r'C:\Users\JT-0919\Desktop\WORK\执法项目\数据\OUTPUT\车辆汇总表.csv'
output_detail_file = r'C:\Users\JT-0919\Desktop\WORK\执法项目\数据\OUTPUT\车辆明细表.csv'

except_file = r'C:\Users\JT-0919\Desktop\WORK\执法项目\数据\审批数据\comm_ALL审批数据.csv'
except_star_file = r'C:\Users\JT-0919\Desktop\WORK\执法项目\数据\审批数据\comm_带星号车.csv'


#  'ch',  'cx',   'rk',   'rksj',    'ck',    'gs',     'cksj'
# '车牌', '车型', '入口站', '入口时间', '出口站', '所在高速', '出口时间'
input_data = pd.DataFrame(data=None, columns=['ch', 'cx', 'rk', 'rksj', 'ck', 'gs', 'cksj'])

input_files = glob.glob(input_file)

# 文件读取
input_data_lst = []
for idx, in_file in enumerate(input_files):
    in_data = pd.read_csv(in_file, encoding="utf-8", keep_default_na=False)
    input_data_lst.append(in_data)
input_data = pd.concat(input_data_lst)
except_data = pd.read_csv(except_file, encoding="utf-8", na_filter=False)
except_star_data = pd.read_csv(except_star_file, encoding="utf-8")

# 过滤条件
set_exp_data = set(except_data['车牌号'].tolist())
set_exp_star_data = set(except_star_data['号牌号码'].tolist())
# '客一', '客二', '客三', '客四'
lst_cx = ['客一']
lst_rk = []
lst_ck = ['北京京沪廊坊站']
set_cx = set(lst_cx)
set_rk = set(lst_rk)
set_ck = set(lst_ck)
filter_cnt = 4

# 统计操作
output_total_data = {}
output_detail_data = []
output_data_cnt = {}
for idx, itr in enumerate(input_data.itertuples(), start=1):
    # print("[%s] Loading [%d/%d]" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), (idx + 1), len(input_data)))

    # sf = ''
    ch = itr[1]
    cx = itr[2]
    rk = itr[3]
    rksj = itr[4]
    ck = itr[5]
    gs = itr[6]
    cksj = itr[7]

    rksj_dt = ''
    rksj_tm = ''
    rksj_hour = ''
    cksj_dt = ''
    cksj_tm = ''
    cksj_hour = ''

    if cx in set_cx:
        continue

    if rk in set_rk:
        continue

    if ck in set_ck:
        continue

    if isinstance(ch, float) or ch == '':
        print('Skip [%s, %s, %s, %s, %s, %s, %s, %s]' % itr)
        continue

    if isinstance(cx, float) or cx == '':
        print('Skip [%s, %s, %s, %s, %s, %s, %s, %s]' % itr)
        continue

    if ch in set_exp_data:
        print('Skip [%s, %s, %s, %s, %s, %s, %s, %s]' % itr)
        continue

    if ch[0:6] in set_exp_star_data:
        print('Skip [%s, %s, %s, %s, %s, %s, %s, %s]' % itr)
        continue

    if isinstance(rksj, str):
        rksj_dt = str.split(rksj, ' ')[0]
        if len(str.split(rksj, ' ')) == 2:
            rksj_tm = str.split(rksj, ' ')[1]
            rksj_tm = str.split(rksj_tm, '.')[0]
            rksj_hour = str.split(rksj_tm, ':')[0]

    if isinstance(cksj, str):
        cksj_dt = str.split(cksj, ' ')[0]
        if len(str.split(cksj, ' ')) == 2:
            cksj_tm = str.split(cksj, ' ')[1]
            cksj_tm = str.split(cksj_tm, '.')[0]
            cksj_hour = str.split(cksj_tm, ':')[0]

    data_key = ch
    data_value = output_total_data.get(data_key)
    data_cnt = 1
    if data_value is None:
        one_data = {
            'ch': ch,
            'cx': cx,
            'jj_cnt': 1,
            'day_cnt': {rksj_dt},
            'rk': {
                rk: {
                    'rk': rk,
                    'rk_cnt': 1,
                    'rksj': {rksj_dt}
                    }
                },
            'ck': {
                ck: {
                    'ck': ck,
                    'ck_cnt': 1,
                    'gs': gs,
                    'cksj': {cksj_dt}
                }
            }
        }
    else:

        jj_cnt = data_value['jj_cnt'] + 1
        day_cnt = data_value['day_cnt']
        day_cnt.add(rksj_dt)

        rk_inf = {}
        ck_inf = {}

        if isinstance(rk, str):
            rk_inf = data_value['rk']
            rk_cnt = 1
            rksj_lst = {rksj_dt}

            for a_rk_inf_key in rk_inf.keys():
                a_rk_inf = rk_inf.get(a_rk_inf_key)
                if rk == a_rk_inf['rk']:
                    rk_cnt = a_rk_inf['rk_cnt'] + 1
                    rksj_lst = a_rk_inf['rksj']
                    rksj_lst.add(rksj_dt)
                    break

            rk_up_inf = {
                rk: {
                    'rk': rk,
                    'rk_cnt': rk_cnt,
                    'rksj': rksj_lst
                }
            }
            rk_inf.update(rk_up_inf)

        if isinstance(ck, str):
            ck_inf = data_value['ck']
            ck_cnt = 1
            cksj_lst = {cksj_dt}

            for a_ck_inf_key in ck_inf.keys():
                a_ck_inf = ck_inf.get(a_ck_inf_key)
                if ck == a_ck_inf['ck']:
                    ck_cnt = a_ck_inf['ck_cnt'] + 1
                    cksj_lst = a_ck_inf['cksj']
                    cksj_lst.add(cksj_dt)
                    break

            ck_up_inf = {
                ck: {
                    'ck': ck,
                    'ck_cnt': ck_cnt,
                    'gs': gs,
                    'cksj': cksj_lst
                }
            }
            ck_inf.update(ck_up_inf)

        one_data = {
            'ch': ch,
            'cx': cx,
            'jj_cnt': jj_cnt,
            'day_cnt': day_cnt,
            'rk': rk_inf,
            'ck': ck_inf
        }
        data_cnt = jj_cnt

    output_total_data.update({data_key: one_data})
    output_data_cnt.update({data_key: data_cnt})
    output_detail_data.append([ch, cx, rk, rksj_dt, rksj_tm, rksj_hour, ck, gs, cksj_dt, cksj_tm, cksj_hour])

# 排序
sorted_data_cnt = sorted(output_data_cnt.items(), key=lambda x: x[1], reverse=True)
sorted_detail_data = sorted(output_detail_data, key=lambda x: (x[0], x[2], x[3], x[4]), reverse=True)

# csv汇总文件输出
csv_total_title = ('序号', '车牌号', '车辆类型',
                   '进京次数', '进京天数', '日平均进京天数',
                   '出京次数', '出京天数',
                   '进京入口', '进京入口活跃次数', '进京入口活跃天数',
                   '进京出口', '进京出口所在高速', '进京出口活跃次数', '进京出口活跃天数')

with open(output_total_file, 'w') as f:
    f.write('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % csv_total_title)

    for idx, a_key in enumerate(sorted_data_cnt):
        # print("[%s] Total Writing [%d/%d]"
        #       % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), (idx + 1), len(output_total_data.keys())))

        # 按次数过滤
        if a_key[1] < filter_cnt:
            continue

        one_data = output_total_data.get(a_key[0])
        f.write('%d,' % (idx + 1))
        f.write('%s,' % one_data.get('ch'))
        f.write('%s,' % one_data.get('cx'))
        f.write('%d,' % one_data.get('jj_cnt'))
        f.write('%d,' % len(one_data.get('day_cnt')))
        f.write('%.1f,' % (one_data.get('jj_cnt') / len(one_data.get('day_cnt'))))
        f.write(',')
        f.write(',')

        multi_rk = False
        multi_ck = False
        cnt_rk = len(one_data.get('rk').keys())
        cnt_ck = len(one_data.get('ck').keys())
        if cnt_rk > 0:
            multi_rk = True
        if cnt_ck > 0:
            multi_ck = True
        loop_cnt = 0
        if cnt_rk > cnt_ck:
            loop_cnt = cnt_rk
        else:
            loop_cnt = cnt_ck

        if cnt_rk > 0 and cnt_ck > 0:
            for i in range(loop_cnt):
                if i == 0:
                    for i_rk, a_rk in enumerate(one_data.get('rk').keys()):
                        one_rk = one_data.get('rk').get(a_rk)
                        f.write('%s,' % one_rk.get('rk'))
                        f.write('%s,' % one_rk.get('rk_cnt'))
                        f.write('%d,' % len(one_rk.get('rksj')))
                        break

                    for i_ck, a_ck in enumerate(one_data.get('ck').keys()):
                        one_ck = one_data.get('ck').get(a_ck)
                        f.write('%s,' % one_ck.get('ck'))
                        f.write('%s,' % one_ck.get('gs'))
                        f.write('%s,' % one_ck.get('ck_cnt'))
                        f.write('%d\n' % len(one_ck.get('cksj')))
                        break
                else:
                    for i_rk, a_rk in enumerate(one_data.get('rk').keys()):
                        if i_rk == i:
                            if i_rk > 0:
                                f.write(",,,,,,,,")
                            one_rk = one_data.get('rk').get(a_rk)
                            f.write('%s,' % one_rk.get('rk'))
                            f.write('%s,' % one_rk.get('rk_cnt'))
                            f.write('%d,' % len(one_rk.get('rksj')))
                            if cnt_rk > cnt_ck and (i >= cnt_ck or cnt_ck == 1):
                                f.write(",,,,\n")
                            break

                    for i_ck, a_ck in enumerate(one_data.get('ck').keys()):
                        if i_ck == i:
                            if cnt_ck > cnt_rk and (i + 1 > cnt_rk or cnt_rk == 1):
                                f.write(",,,,,,,,,,,")
                            one_ck = one_data.get('ck').get(a_ck)
                            f.write('%s,' % one_ck.get('ck'))
                            f.write('%s,' % one_ck.get('gs'))
                            f.write('%s,' % one_ck.get('ck_cnt'))
                            f.write('%d\n' % len(one_ck.get('cksj')))
                            break

        elif cnt_rk > 0 and cnt_ck == 0:
            for i_rk, a_rk in enumerate(one_data.get('rk').keys()):

                if multi_rk and i_rk > 0:
                    f.write(",,,,,,,,")
                one_rk = one_data.get('rk').get(a_rk)
                f.write('%s,' % one_rk.get('rk'))
                f.write('%s,' % one_rk.get('rk_cnt'))
                f.write('%d,' % len(one_rk.get('rksj')))
                f.write(",,,,\n")

        elif cnt_rk == 0 and cnt_ck > 0:
            for i_ck, a_ck in enumerate(one_data.get('ck').keys()):

                if multi_ck and i_ck > 0:
                    f.write(",,,,,,,,,,")
                f.write(",,,")
                one_ck = one_data.get('ck').get(a_ck)
                f.write('%s,' % one_ck.get('ck'))
                f.write('%s,' % one_ck.get('gs'))
                f.write('%s,' % one_ck.get('ck_cnt'))
                f.write('%d\n' % len(one_ck.get('cksj')))

        else:
            f.write(',,,,,,,,,,,,\n')


# csv明细文件输出
csv_detail_title = ('编号', '车牌', '车型',
                   '入口', '入口日期', '入口时间', '入口几点',
                   '出口', '所属高速', '出口日期', '出口时间', '出口几点')

with open(output_detail_file, 'w') as f:
    f.write('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % csv_detail_title)

    for idx, a_detail_data in enumerate(sorted_detail_data):
        # print("[%s] Detail Writing [%d/%d]"
        #       % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), (idx + 1), len(sorted_detail_data)))

        if idx == 42:
            print()
        f.write('%d,' % (idx + 1))
        f.write(','.join(a_detail_data))
        f.write('\n')
