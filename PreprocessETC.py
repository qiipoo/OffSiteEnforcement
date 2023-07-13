# -*- coding: utf-8 -*-
import datetime
import pandas as pd
import glob
import os

etc_path = r'C:\Users\Administrator\Desktop\lkyw\ETC_2303\etc_tmp_202303'
new_etc_path = r'C:\Users\Administrator\Desktop\lkyw\ETC_2303\new'
output_etc_path = r'C:\Users\Administrator\Desktop\lkyw\ETC_2303\output'
history_punished_path = r'C:\Users\Administrator\Desktop\lkyw\Approval\history_punished_data_all.xlsx'

cktx_title = '通行标识ID,出口日期及时间,出口实际车辆车牌号,出口识别车辆车牌号,出口计费车型代码,出口车种代码,出口站编号,出口站名称,计费总里程数,支付类型,通行省份个数,出口站经度,出口站纬度,出口所在高速'
rktx_title = '通行标识ID,入口日期及时间,入口实际车牌号,入口识别车牌号,入口车型代码,入口车种代码,入口站编号,入口站名称,入口站经度,入口站纬度,入口所在高速'
mjtx_title = '通行标识ID,门架编号,行驶方向,通过时间,计费车辆车牌号,识别车辆车牌号,计费车型代码,计费车种代码,门架类型,门架名称,门架经度,门架纬度,门架所在高速'

# etc_rktx_file_title = r'通行标识ID,通行介质类型,OBU序号编码,通行介质编码,入口日期及时间,入口实际车牌号,入口识别车牌号,入口车型代码,入口车种代码,入口车货总重,入口轴数,入口站编号,入口站名称,入口车道编号,入口车道类型'
# etc_mjtx_file_title = r'通行标识ID,门架编号,门架HEX字符串,行驶方向,门架顺序号,门架类型,对向门架Hex值,通过时间,通行介质类型,OBU序号编码,通行介质编码,计费车辆车牌号,识别车辆车牌号,计费车型代码,计费车种代码,车轴数,车辆座位数/载重,车道编号,车辆速度,入口站hex字符串,入口名称,入口日期及时间,入口状态,入口车货总重,上一个门架的HEX字符串,通过上一个门架的时间'
# etc_cktx_file_title = r'通行标识ID,通行介质类型,计费方式,OBU序号编码,通行介质编码,出口日期及时间,出口实际车辆车牌号,出口识别车辆车牌号,出口计费车型代码,出口车种代码,出口车货总重,出口轴数,出口站编号,出口站名称,出口车道的编号,出口车道类型,入口编号,入口名称,入口时间,入口车货总重,是否开放式收费站,通行省份个数'



def func_file_preprocess(old_file, new_file, title=''):
    with open(old_file, 'r', encoding='utf-8') as old:
        old_data = old.read()
    with open(new_file, 'w', encoding='utf-8') as new:
        new.write(title + "\n" + old_data)


def func_check_vehicle(vehicle_no, set_vehicles):
    if vehicle_no in set_vehicles:
        return 1
    else:
        return 0


if __name__ == '__main__':
    start_time = datetime.datetime.now()

    os.makedirs(new_etc_path, exist_ok=True)
    etc_files = glob.glob(os.path.join(etc_path, '*.csv'))

    etc_date = {}
    for idx, in_file in enumerate(etc_files):
        file_name = in_file.split('\\')[-1]
        new_file = os.path.join(new_etc_path, file_name)
        etc_date.update({file_name[5:13]: file_name[5:13]})
        if file_name[:4] == 'RKTX':
            func_file_preprocess(in_file, new_file, rktx_title)
        elif file_name[:4] == 'MJTX':
            func_file_preprocess(in_file, new_file, mjtx_title)
        elif file_name[:4] == 'CKTX':
            func_file_preprocess(in_file, new_file, cktx_title)

    new_etc_files = glob.glob(os.path.join(new_etc_path, '*.csv'))

    history_punished_data = pd.read_excel(history_punished_path)
    set_hp_vehicle = set(history_punished_data['车牌号'].tolist())

    for a_date in etc_date:

        cktx = pd.read_csv(os.path.join(new_etc_path, 'CKTX_' + a_date + '.csv'))
        rktx = pd.read_csv(os.path.join(new_etc_path, 'RKTX_' + a_date + '.csv'))
        mjtx = pd.read_csv(os.path.join(new_etc_path, 'MJTX_' + a_date + '.csv'))

        cktx['通行标识ID'] = cktx['通行标识ID'].astype(str)
        rktx['通行标识ID'] = rktx['通行标识ID'].astype(str)
        mjtx['通行标识ID'] = mjtx['通行标识ID'].astype(str)

        cktx.insert(4, '出口实际车辆车牌颜色', '')
        cktx.insert(8, '出口站点类型', '收费站')
        rktx.insert(4, '入口实际车牌颜色', '')
        rktx.insert(8, '入口站点类型', '收费站')
        mjtx.insert(8, '站点类型', '省界门架')

        merged_df = pd.merge(cktx, rktx, on='通行标识ID', how='outer')
        merged_df.drop_duplicates(inplace=True)
        merged_df['通行标识ID'].replace('nan', pd.NA, inplace=True)
        merged_df.dropna(subset=['通行标识ID'], inplace=True)

        merged_df = pd.merge(merged_df, mjtx, on='通行标识ID', how='left')

        merged_df.loc[merged_df['入口日期及时间'].isnull(), '入口日期及时间'] = merged_df['通过时间']
        merged_df.loc[merged_df['入口实际车牌号'].isnull(), '入口实际车牌号'] = merged_df['计费车辆车牌号']
        merged_df.loc[merged_df['入口识别车牌号'].isnull(), '入口识别车牌号'] = merged_df['识别车辆车牌号']
        merged_df.loc[merged_df['入口车型代码'].isnull(), '入口车型代码'] = merged_df['计费车型代码']
        merged_df.loc[merged_df['入口车种代码'].isnull(), '入口车种代码'] = merged_df['计费车种代码']
        merged_df.loc[merged_df['入口站编号'].isnull(), '入口站编号'] = merged_df['门架编号']
        merged_df.loc[merged_df['入口站名称'].isnull(), '入口站名称'] = merged_df['门架名称']
        merged_df.loc[merged_df['入口站经度'].isnull(), '入口站经度'] = merged_df['门架经度']
        merged_df.loc[merged_df['入口站纬度'].isnull(), '入口站纬度'] = merged_df['门架纬度']
        merged_df.loc[merged_df['入口所在高速'].isnull(), '入口所在高速'] = merged_df['门架所在高速']
        merged_df.loc[merged_df['入口站点类型'].isnull(), '入口站点类型'] = merged_df['站点类型']

        merged_df.loc[merged_df['出口日期及时间'].isnull(), '出口日期及时间'] = merged_df['通过时间']
        merged_df.loc[merged_df['出口实际车辆车牌号'].isnull(), '出口实际车辆车牌号'] = merged_df['计费车辆车牌号']
        merged_df.loc[merged_df['出口识别车辆车牌号'].isnull(), '出口识别车辆车牌号'] = merged_df['识别车辆车牌号']
        merged_df.loc[merged_df['出口计费车型代码'].isnull(), '出口计费车型代码'] = merged_df['计费车型代码']
        merged_df.loc[merged_df['出口车种代码'].isnull(), '出口车种代码'] = merged_df['计费车种代码']
        merged_df.loc[merged_df['出口站编号'].isnull(), '出口站编号'] = merged_df['门架编号']
        merged_df.loc[merged_df['出口站名称'].isnull(), '出口站名称'] = merged_df['门架名称']
        merged_df.loc[merged_df['出口站经度'].isnull(), '出口站经度'] = merged_df['门架经度']
        merged_df.loc[merged_df['出口站纬度'].isnull(), '出口站纬度'] = merged_df['门架纬度']
        merged_df.loc[merged_df['出口所在高速'].isnull(), '出口所在高速'] = merged_df['门架所在高速']
        merged_df.loc[merged_df['出口站点类型'].isnull(), '出口站点类型'] = merged_df['站点类型']

        merged_df.drop(['通过时间', '计费车辆车牌号', '识别车辆车牌号', '计费车型代码', '计费车种代码', '门架编号', '门架名称',
                        '门架经度', '门架纬度', '门架所在高速', '行驶方向', '门架类型', '站点类型'], axis=1, inplace=True)

        merged_df['入口日期及时间'].replace('nan', pd.NA, inplace=True)
        merged_df['入口实际车牌号'].replace('nan', pd.NA, inplace=True)
        merged_df['出口日期及时间'].replace('nan', pd.NA, inplace=True)
        merged_df['出口实际车辆车牌号'].replace('nan', pd.NA, inplace=True)

        merged_df.dropna(subset=['入口日期及时间'], inplace=True)
        merged_df.dropna(subset=['入口实际车牌号'], inplace=True)
        merged_df.dropna(subset=['出口日期及时间'], inplace=True)
        merged_df.dropna(subset=['出口实际车辆车牌号'], inplace=True)

        merged_df['出口实际车辆车牌颜色'] = merged_df['出口实际车辆车牌号'].apply(lambda x: str.split(x, '_')[1])
        merged_df['出口实际车辆车牌号'] = merged_df['出口实际车辆车牌号'].apply(lambda x: str.split(x, '_')[0])
        merged_df['入口实际车牌颜色'] = merged_df['入口实际车牌号'].apply(lambda x: str.split(x, '_')[1])
        merged_df['入口实际车牌号'] = merged_df['入口实际车牌号'].apply(lambda x: str.split(x, '_')[0])

        merged_df['是否有过违法行为'] = merged_df['出口实际车辆车牌号'].apply(lambda x: func_check_vehicle(x, set_hp_vehicle))

        os.makedirs(output_etc_path, exist_ok=True)
        merged_df.to_csv(os.path.join(output_etc_path, 'ETC_' + a_date + '.csv'), index=False)

    end_time = datetime.datetime.now()
    print('Process used %d seconds!' % (end_time - start_time).seconds)
