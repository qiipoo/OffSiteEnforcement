# -*- coding: utf-8 -*-

import mariadb

def mariadb_test():
    conn = mariadb.connect(user="root", password="admin", host="127.0.0.1", port=3306, database="zhifa")

    cur = conn.cursor()

    sql = "" \
          "select t1.车牌号, t1.cnt as '入口次数', t2.cnt as '出口次数', t3.cnt as '门架次数', " \
          "t1.cnt + t2.cnt + t3.cnt as '总计' " \
          "from " \
          " (select 入口实际车牌号 车牌号, count(1) as cnt from rktx " \
          "	where 入口实际车牌号 != '默A00000' " \
          "		and date(入口日期及时间) >= date_sub('2022-6-30', INTERVAL 7 DAY) " \
          "		and 入口实际车牌号 not in (select 牌照号 from bus_carno_list bcl) " \
          "	group by 入口实际车牌号 " \
          "	order by cnt desc) t1, " \
          "	(select 出口实际车辆车牌号 车牌号, count(1) as cnt from cktx " \
          "	where 出口实际车辆车牌号 != '默A00000' " \
          "		and date(出口日期及时间) >= date_sub('2022-6-30', INTERVAL 7 DAY) " \
          "		and 出口实际车辆车牌号 not in (select 牌照号 from bus_carno_list bcl) " \
          "	group by 出口实际车辆车牌号 " \
          "	order by cnt desc) t2, " \
          "	(select 识别车辆车牌号 车牌号, count(1) as cnt from mjtx " \
          "	where 识别车辆车牌号 != '默A00000' " \
          "		and date(通过时间) >= date_sub('2022-6-30', INTERVAL 7 DAY) " \
          "		and 识别车辆车牌号 not in (select 牌照号 from bus_carno_list bcl) " \
          "	group by 识别车辆车牌号 " \
          "	order by cnt desc) t3 " \
          "where t1.车牌号 = t2.车牌号 and t1.车牌号 = t3.车牌号 and t3.cnt > 0 " \
          "	and (t1.cnt + t2.cnt + t3.cnt) >= 3 " \
          "order by t1.cnt + t2.cnt + t3.cnt desc "

    sql = "" \
          "select t1.车牌号, t1.cnt as '入口次数', t2.cnt as '出口次数', '' as '门架次数', " \
          "t1.cnt + t2.cnt as '总计' " \
          "from " \
          " (select 入口实际车牌号 车牌号, count(1) as cnt from rktx " \
          "	where 入口实际车牌号 != '默A00000' " \
          "		and 入口车型代码 in (1,2,3, 4) " \
          "		and 入口实际车牌号 not in (select 牌照号 from bus_carno_list bcl) " \
          "	group by 入口实际车牌号 " \
          "	order by cnt desc) t1, " \
          "	(select 出口实际车辆车牌号 车牌号, count(1) as cnt from cktx " \
          "	where 出口实际车辆车牌号 != '默A00000' " \
          "		and 出口计费车型代码  in (1,2, 3, 4) " \
          "		and 出口实际车辆车牌号 not in (select 牌照号 from bus_carno_list bcl) " \
          "	group by 出口实际车辆车牌号 " \
          "	order by cnt desc) t2 " \
          "where t1.车牌号 = t2.车牌号 " \
          "	and (t1.cnt + t2.cnt) >= 3 " \
          "order by t1.cnt + t2.cnt desc "

    cur.execute(sql)
    result = cur.fetchall()

    total_csv_title = ('序号', '车牌号', '车型', '入口次数', '出口次数', '门架次数', '总计', '出现门架名称/收费站', '所在高速(收费站)')
    filepath = 'C:\\Users\\JT-0919\\Desktop\\WORK\\执法项目\\数据\\OUTPUT\\汇总表-用于筛选车辆的汇总表.csv'
    with open(filepath, 'w') as f:
        f.write("%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % total_csv_title)
        cnt = 1
        for one_rec in result:
            print("%d/%d" %(cnt, len(result)))
            sql = "select distinct DECODE_ORACLE(t1.入口车型代码, 1, '客一', 2, '客二', 3, '客三', 4, '客四') 车型, " \
                  "t1.收费站名称, t1.收费站所属线路 " \
                  "from rktx t1 " \
                  "where t1.入口实际车牌号 = '" + one_rec[0] + "' " \
                  "order by t1.收费站名称 "
            cur.execute(sql)
            rk_ret = cur.fetchall()

            sql = "select distinct t1.收费站名称, t1.收费站所属线路 " \
                  "from cktx t1 " \
                  "where t1.出口实际车辆车牌号 = '" + one_rec[0] + "' " \
                  "order by t1.收费站名称 "
            cur.execute(sql)
            ck_ret = cur.fetchall()

            sql = "select distinct t1.门架名称 " \
                  "from mjtx t1 " \
                  "where t1.识别车辆车牌号 = '" + one_rec[0] + "' " \
                  "order by t1.门架名称 "
            cur.execute(sql)
            mj_ret = cur.fetchall()

            for idx, one_rk in enumerate(rk_ret, start=0):
                if idx == 0:
                    f.write("%d," % cnt)
                    f.write("%s," % one_rec[0])
                    f.write("%s," % one_rk[0])
                    f.write("%s,%s,%s,%s," % one_rec[1:5])
                    f.write("%s,%s\n" % one_rk[1:3])
                else:
                    f.write(",,,,,,,%s,%s\n" % one_rk[1:3])

            for idx, one_ck in enumerate(ck_ret, start=0):
                f.write(",,,,,,,%s,%s\n" % one_ck)

            for idx, one_mj in enumerate(mj_ret, start=0):
                f.write(",,,,,,,%s,\n" % one_mj)

            cnt = cnt + 1

    # tongxing_csv_title = ('序号', '车牌号', '通行标识ID', '通行时间', '门架名称/收费站', '所在高速', '类型')
    # filepath = 'C:\\Users\\JT-0919\\Desktop\\WORK\\执法项目\\数据\\OUTPUT\\通行记录-给执法人员看的表格.csv'
    # with open(filepath, 'w') as f:
    #     f.write("%s,%s,%s,%s,%s,%s,%s\n" % tongxing_csv_title)
    #     cnt = 1
    #
    # rk_csv_title = ('通行标识ID', '入口日期及时间', '入口实际车牌号', '入口车型代码', '入口站编号', '入口站名称', '高速公路名称',
    #                 '收费站经度', '收费站纬度')
    # filepath = 'C:\\Users\\JT-0919\\Desktop\\WORK\\执法项目\\数据\\OUTPUT\\通行记录-给执法人员看的表格.csv'
    # with open(filepath, 'w') as f:
    #     f.write("%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % rk_csv_title)
    #     cnt = 1
    #
    # ck_csv_title = ('通行标识ID', '出口日期及时间', '出口实际车辆车牌号', '出口计费车型代码', '出口站编号', '出口站名称',
    #                 '高速公路名称', '收费站经度', '收费站纬度')
    # filepath = 'C:\\Users\\JT-0919\\Desktop\\WORK\\执法项目\\数据\\OUTPUT\\通行记录-给执法人员看的表格.csv'
    # with open(filepath, 'w') as f:
    #     f.write("%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % ck_csv_title)
    #     cnt = 1
    #
    # mj_csv_title = ('通行标识ID', '门架编号', '通过时间', '识别车辆车牌号', '计费车型代码', '入口通行标识ID', '出口通行标识ID',
    #                 '当前门架名称', '经度', '纬度')
    # filepath = 'C:\\Users\\JT-0919\\Desktop\\WORK\\执法项目\\数据\\OUTPUT\\通行记录-给执法人员看的表格.csv'
    # with open(filepath, 'w') as f:
    #     f.write("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % mj_csv_title)
    #     cnt = 1


    cur.close()
    conn.close()
