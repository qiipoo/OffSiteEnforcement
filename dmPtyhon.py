
import dmPython


dmProperties = {
    'user': 'ZHANGXL',
    'password': '6Y,LmU7gC290|4%OKbBvhx3kHew1$Z_/',
    'server': '172.26.57.210',
    'port': 5236,
    'autoCommit': False,
    }

dmConn = dmPython.connect(**dmProperties)

dmCursor = dmConn.cursor()
dmCursor.execute("select * from GXJTW.LONGDISTANCE_BUS")

dmResult = dmCursor.fetchall()

for a_rec in dmResult:
    print(a_rec)



