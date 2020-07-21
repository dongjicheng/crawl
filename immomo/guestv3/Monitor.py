# -*- coding: utf-8 -*-
import time
import Queue
import threading
from WorkProcess import WorkProcess

class ThreadMonitor(threading.Thread):
    def __init__(self, taskcount):
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.TaskCount = taskcount

    def run(self):
        prevTime = 0
        prevCount = WorkProcess.complete.value

        while True:
            dt = time.time() - prevTime
            end = time.time() + 30

            with WorkProcess.lock:
                status = len(WorkProcess.status)
                if WorkProcess.status:
                    good = reduce(lambda x,y:x+y,WorkProcess.status.values())
                else:
                    good = 0
            print '%s Download %d urls (at %.2f url/min), Queue %d tasks, lines %d/%d' % \
                    (time.strftime('%Y-%m-%d %H:%M:%S'), 
                     WorkProcess.complete.value, 
                     60*(WorkProcess.complete.value - prevCount)/dt, 
                     self.TaskCount.value, 
                     good,
                     status)
            prevCount = WorkProcess.complete.value
            prevTime = time.time()

            if time.time() < end:
                threading._sleep(end - time.time())
