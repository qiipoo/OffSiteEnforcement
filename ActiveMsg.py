import time
import sys
import stomp



class GPSListener(stomp.ConnectionListener):
    def on_error(self, frame):
        print('received an error "%s"' % frame.body)

    def on_message(self, frame):
        print('received a message "%s"' % frame.body)


if __name__ == '__main__':
    try:
        conn = stomp.Connection([('172.26.53.233', 61613)])
        conn.set_listener('gpsListener', GPSListener())
        conn.connect('admin', '3bfa9a8f4a9', wait=True)
        conn.subscribe(destination='/gps_topic_message', id='1', ack='auto')
        # conn.send(body=' '.join(sys.argv[1:]), destination='/queue/test')
        time.sleep(2)
        conn.disconnect()
    except Exception as e:
        print(e)
