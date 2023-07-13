import numpy as np
import pandas as pd
import json

json_obj1 = {
    "lot": "20230601134503",
    "data": [{
        "transport_certificate": "20230536",
        "vehicle_no": "冀A88869",
        "plate_color": "黄",
        "terminal_type": "",
        "terminal_platform": "",
        "company_name": "石家庄运输集团有限公司",
        "registered_area": "西城区",
        "longdistance_bus_id": "73002278",
        "departure_date": "20230328",
        "gps_date": "20230328",
        "etc_date": "20230328",
        "no_gps_days": "1",
        "last_gps_date": "20230327"
    },
        {
            "transport_certificate": "省110106000949",
            "vehicle_no": "京AAZ789",
            "plate_color": "黄",
            "company_name": "北京高客长途客运有限责任公司",
            "registered_area": "西城区",
            "terminal_type": "BSJ_A6BD",
            "terminal_platform": "天泰卫星定位运营服务平台",
            "longdistance_bus_id": "73395686",
            "departure_date": "20230328",
            "gps_date": "20230328",
            "etc_date": "20230328",
            "no_gps_days": "1",
            "last_gps_date": ""
        }]
}

json_obj2 = {
    "lot": "20230605134503",
    "data": [{
        "transport_certificate": "省110105000628",
        "vehicle_no": "京AN2160",
        "plate_color": "黄色",
        "company_name": "交通国际旅行社有限公司",
        "contact": "金珊珊",
        "tel": "",
        "phone": "13716391883",
        "registered_area": "朝阳区",
        "rating_overdue_days": "128",
        "rating_deadline": "20230131",
        "departure_date ": "20230508",
        "etc_date": "20230508",
        "gps_date": "20230508"
    },
        {
            "transport_certificate": "省110106001136",
            "vehicle_no": "京AL7273",
            "plate_color": "黄色",
            "company_name": "北京国汇兴客运有限公司",
            "contact": "王镔",
            "tel": "",
            "phone": "13522149728",
            "registered_area": "丰台区",
            "rating_overdue_days": "100",
            "rating_deadline": "20230331",
            "departure_date ": "20230508",
            "etc_date": "20230508",
            "gps_date": "20230508"
        }]
}

json_obj3 = {
    "lot": "20230614170502",
    "data": [{
        "vehicle_no": "京AN4197",
        "days": "11",
        "start_date": "20230318",
        "end_date": "20230328",
        "business_scope": "省际班车",
        "origin_destination": "四惠-海城",
        "company_name": "新国线运输集团有限公司",
        "contact": "李胜超",
        "tel": "83715895",
        "phone": "15313907601",
        "registered_area": "海淀区",
        "issuing_auth": "北京市交通委员会西城运输管理分局"
    }, {
        "vehicle_no": "京ER6638",
        "days": "15",
        "start_date": "20230307",
        "end_date": "20230321",
        "business_scope": "省际班车",
        "origin_destination": "新发地-建湖",
        "company_name": "北京新月联合汽车有限公司",
        "contact": "王镔",
        "tel": "",
        "phone": "13810419805",
        "registered_area": "丰台区",
        "issuing_auth": "北京市交通委员会丰台运输管理分局"
    }]
}

json_obj4 = {
    "lot": "20230613131211",
    "data": [{
        "vehicle_no": "京ANW805",
        "into_cnt": "1",
        "into_days": "1",
        "avg_per_day_into": "1",
        "out_cnt": "1",
        "out_days": "1",
        "avg_per_day_out": "1",
        "motorway_cnt": "37",
        "motorway_days": "20",
        "avg_per_day_motor": "1.9",
        "max_usage_route": "北京六环马驹桥西站-北京郎府站",
        "max_usage_route_cnt": "18",
        "history_illegality_cnt": "1",
        "is_lease": "0",
        "is_b2nb": "0",
        "is_high_freq": "0",
        "composite_score": "0.458205128"
    },
        {
            "vehicle_no": "京AFA145",
            "into_cnt": "21",
            "into_days": "21",
            "avg_per_day_into": "1",
            "out_cnt": "22",
            "out_days": "22",
            "avg_per_day_out": "1",
            "motorway_cnt": "0",
            "motorway_days": "0",
            "avg_per_day_motor": "0",
            "max_usage_route": "北京白鹿主站-北京京哈高速互通-北京香河站1",
            "max_usage_route_cnt": "22",
            "history_illegality_cnt": "2",
            "is_lease": "0",
            "is_b2nb": "0",
            "is_high_freq": "1",
            "composite_score": "0.418974359"
        }]
}


json_str1 = json.dumps(json_obj1.get('data'))
json_str2 = json.dumps(json_obj2.get('data'))
json_str3 = json.dumps(json_obj3.get('data'))
json_str4 = json.dumps(json_obj4.get('data'))

df1 = pd.read_json(json_str1, orient='records')
df2 = pd.read_json(json_str2, orient='records')
df3 = pd.read_json(json_str3, orient='records')
df4 = pd.read_json(json_str4, orient='records')

df1.to_csv(r'C:\Users\JT-0919\Downloads\sj_gps_offline.csv', index=False)
df2.to_csv(r'C:\Users\JT-0919\Downloads\sj_rating_overdue.csv', index=False)
df3.to_csv(r'C:\Users\JT-0919\Downloads\sj_long_term_not_in_registered_area.csv', index=False)
df4.to_csv(r'C:\Users\JT-0919\Downloads\sj_black_vehicle.csv', index=False)
