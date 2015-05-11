import signal
import time

class Alarm(Exception):
    pass

def alarm_handler(signum, frame):
    raise Alarm

signal.signal(signal.SIGALRM, alarm_handler)
signal.alarm(5)
while(1):
    try:
        while(1):
            time.sleep(0.9)
            print 'sleep...'
            pass
    except Alarm:
        print 'what is this!'
        signal.alarm(5)
