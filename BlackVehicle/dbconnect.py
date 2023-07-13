# -*- coding: utf-8 -*-
import time
import mariadb
import datetime


def mariadb_test():
    conn = mariadb.connect(user="root", password="admin", host="127.0.0.1", port=3306, database="zhifa")

    cur = conn.cursor()

    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    car_no = ''

    sql1 = "" \
           "select t1.ch, t1.cx, count(1) 进京天数 " \
           "from 一个月数据 t1 " \
           "where t1.ck != '北京京沪廊坊站' " \
           "and t1.cx  != '客四' " \
           "group by t1.ch, t1.cx " \
           "order by 进京天数 desc"

    cur.execute(sql1)
    result = cur.fetchall()

    total_csv_title = ('序号', '车牌号', '车辆类型',
                       '进京次数', '进京天数', '日平均进京次数',
                       '出京次数', '出京天数',
                       '进京入口', '进京入口活跃次数', '进京入口活跃天数',
                       '进京出口', '进京出口所在高速', '进京出口活跃次数', '进京出口活跃天数')
    filepath = r'C:\Users\JT-0919\Desktop\WORK\执法项目\数据\OUTPUT\车辆汇总表.csv'
    with open(filepath, 'w') as f:
        f.write("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % total_csv_title)
        cnt = 1
        for one_rec in result:
            print("%d/%d" % (cnt, len(result)))
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
            car_no = one_rec[0]
            car_typ = one_rec[1]
            # 进京天数
            sql2 = "" \
                   "select tt1.ch, count(1) 进京天数 " \
                   "from ( " \
                   "select t1.ch, date_format(t1.rksj, '%Y/%m/%d') rksj " \
                   "from 一个月数据 t1 " \
                   "where t1.ch = '" + car_no + "' " \
                   "and t1.cx = '" + car_typ + "' " \
                   "group by t1.ch, date_format(t1.rksj, '%Y/%m/%d') " \
                   "order by t1.ch, date_format(t1.rksj, '%Y/%m/%d') " \
                   ") tt1 " \
                   "group by tt1.ch " \
                   "order by  进京天数 desc"

            # 进京天数
            cur.execute(sql2)
            ret2 = cur.fetchall()
            day_avg = one_rec[2] / ret2[0][1]
            if day_avg < 2:
                continue

            # 出京天数
            sql3 = ""

            # 进京入口, 进京入口活跃次数
            sql4 = "" \
                   "select t1.rk, count(1) 进京次数 " \
                   "from 一个月数据 t1 " \
                   "where t1.ch = '" + car_no + "' " \
                   "and t1.cx = '" + car_typ + "' " \
                   "and t1.rk <> '' " \
                   "group by t1.rk "

            # 进京出口, 进京出口活跃次数
            sql6 = "" \
                   "select t1.ck, t1.gs, count(1) 出京次数 " \
                   "from 一个月数据 t1 " \
                   "where t1.ch = '" + car_no + "' " \
                   "and t1.cx = '" + car_typ + "' " \
                   "and t1.ck <> '' " \
                   "group by t1.ck,t1.gs "





            # 出京天数
            # cur.execute(sql3)
            # ret3 = cur.fetchall()

            # 进京入口, 进京入口活跃次数
            cur.execute(sql4)
            ret4 = cur.fetchall()

            # 进京出口, 进京出口活跃次数
            cur.execute(sql6)
            ret6 = cur.fetchall()

            f.write("%d," % cnt)
            f.write("%s," % one_rec[0])
            f.write("%s," % one_rec[1])
            f.write("%s," % one_rec[2])
            f.write("%s," % ret2[0][1])
            f.write("%s," % day_avg)
            f.write(",,")

            ret4_len = len(ret4)
            ret6_len = len(ret6)
            multi_rk = False
            multi_ck = False
            if ret4_len > 0:
                multi_rk = True
            if ret6_len > 0:
                multi_ck = True
            loop_cnt = 0
            if ret4_len > ret6_len:
                loop_cnt = ret4_len
            else:
                loop_cnt = ret6_len

            if ret4_len > 0 and ret6_len > 0:
                for i in range(loop_cnt):
                    if i == 0:
                        for idx, one_ret in enumerate(ret4, start=0):
                            in_st = one_ret[0]
                            # 进京入口, 进京入口活跃天数
                            sql5 = "" \
                                   "select distinct date_format(t1.rksj, '%Y/%m/%d') " \
                                   "from 一个月数据 t1 " \
                                   "where t1.ch = '" + car_no + "' " \
                                   "and t1.cx = '" + car_typ + "' " \
                                   "and t1.rk = '" + in_st + "' " \
                                # 进京入口, 进京入口活跃天数
                            cur.execute(sql5)
                            ret5 = cur.fetchall()
                            f.write("%s,%s," % one_ret)
                            f.write("%s," % len(ret5))
                            break

                        for idx, one_ret in enumerate(ret6, start=0):
                            out_st = one_ret[0]
                            # 进京出口, 进京出口活跃天数
                            sql7 = "" \
                                   "select distinct date_format(t1.cksj, '%Y/%m/%d') " \
                                   "from 一个月数据 t1 " \
                                   "where t1.ch = '" + car_no + "' " \
                                   "and t1.cx = '" + car_typ + "' " \
                                   "and t1.ck = '" + out_st + "' "
                            # 进京出口, 进京出口活跃天数
                            cur.execute(sql7)
                            ret7 = cur.fetchall()
                            f.write("%s,%s,%s," % one_ret)
                            f.write("%s\n" % len(ret7))
                            break
                    else:
                        have_rk = False
                        for idx, one_ret in enumerate(ret4, start=0):
                            if idx == i:
                                in_st = one_ret[0]
                                # 进京入口, 进京入口活跃天数
                                sql5 = "" \
                                       "select distinct date_format(t1.rksj, '%Y/%m/%d') " \
                                       "from 一个月数据 t1 " \
                                       "where t1.ch = '" + car_no + "' " \
                                       "and t1.cx = '" + car_typ + "' " \
                                       "and t1.rk = '" + in_st + "' " \
                                    # 进京入口, 进京入口活跃天数
                                cur.execute(sql5)
                                ret5 = cur.fetchall()

                                if idx > 0:
                                    f.write(",,,,,,,,")
                                    have_rk = True
                                f.write("%s,%s," % one_ret)
                                f.write("%s," % len(ret5))
                                if ret4_len > ret6_len and ((idx+1) == ret6_len or ret6_len == 1):
                                    f.write(",,,,\n")
                                    break

                        for idx, one_ret in enumerate(ret6, start=0):
                            if idx == i:
                                out_st = one_ret[0]
                                # 进京出口, 进京出口活跃天数
                                sql7 = "" \
                                       "select distinct date_format(t1.cksj, '%Y/%m/%d') " \
                                       "from 一个月数据 t1 " \
                                       "where t1.ch = '" + car_no + "' " \
                                       "and t1.cx = '" + car_typ + "' " \
                                       "and t1.ck = '" + out_st + "' "
                                # 进京出口, 进京出口活跃天数
                                cur.execute(sql7)
                                ret7 = cur.fetchall()
                                if have_rk == False and idx > 0:
                                    have_rk = False
                                    f.write(",,,,,,,,,,,")
                                if ret6_len > ret4_len and not have_rk == False and (idx + 1) > ret4_len and idx > 0:
                                    have_rk = False
                                    f.write(",,,,,,,,,,,")
                                f.write("%s,%s,%s," % one_ret)
                                f.write("%s\n" % len(ret7))

            elif ret4_len > 0 and ret6_len == 0:
                for idx, one_ret in enumerate(ret4, start=0):
                    in_st = one_ret[0]
                    # 进京入口, 进京入口活跃天数
                    sql5 = "" \
                           "select distinct date_format(t1.rksj, '%Y/%m/%d') " \
                           "from 一个月数据 t1 " \
                           "where t1.ch = '" + car_no + "' " \
                           "and t1.cx = '" + car_typ + "' " \
                           "and t1.rk = '" + in_st + "' " \
                        # 进京入口, 进京入口活跃天数
                    cur.execute(sql5)
                    ret5 = cur.fetchall()

                    if multi_rk and idx > 0:
                        f.write(",,,,,,,,")
                    f.write("%s,%s," % one_ret)
                    f.write("%s," % len(ret5))
                    f.write(",,,,\n")

            elif ret4_len == 0 and ret6_len > 0:
                for idx, one_ret in enumerate(ret6, start=0):
                    out_st = one_ret[0]
                    # 进京出口, 进京出口活跃天数
                    sql7 = "" \
                           "select distinct date_format(t1.cksj, '%Y/%m/%d') " \
                           "from 一个月数据 t1 " \
                           "where t1.ch = '" + car_no + "' " \
                           "and t1.cx = '" + car_typ + "' " \
                           "and t1.ck = '" + out_st + "' "
                    # 进京出口, 进京出口活跃天数
                    cur.execute(sql7)
                    ret7 = cur.fetchall()

                    if multi_ck and idx > 0:
                        f.write(",,,,,,,,,,")
                    f.write(",,,")
                    f.write("%s,%s,%s," % one_ret)
                    f.write("%s\n" % len(ret7))

            else:
                f.write(",,,,,,,")

            cnt = cnt + 1

    cur.close()
    conn.close()
