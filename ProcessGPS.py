import os
import sys
import glob
from datetime import datetime, timedelta
import shutil

if __name__ == '__main__':

    if len(sys.argv) > 1:
        day_range = int(sys.argv[1])
    else:
        day_range = 7

    today = datetime.today().date().strftime('%Y%m%d')
    yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')
    last_day = (datetime.today() - timedelta(days=day_range)).strftime('%Y%m%d')

    dst_gps_files = glob.glob('/data/sftp/*.csv')
    for idx, in_file in enumerate(dst_gps_files):
        file_name = in_file.split('/')[-1]
        if file_name.find('GPS_') > -1:
            gps_date = file_name.split('_')[-1].split('.')[0]
            if gps_date < last_day:
                os.remove(in_file)

    src_gps_files = glob.glob('/data/ActiveMQ/file/*.csv')
    for idx, in_file in enumerate(src_gps_files):
        file_name = in_file.split('/')[-1]
        if file_name.find('GPS_') > -1:
            gps_date = file_name.split('_')[-1].split('.')[0]
            if last_day <= gps_date < today:
                shutil.copy(in_file, '/data/sftp/')
