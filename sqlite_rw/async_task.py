# encoding=utf-8

import threading
import logging
import time
import traceback
from collections import deque

class AsyncThread(threading.Thread):
    
    MAX_TASK_QUEUE = 200
    lock = threading.RLock()
    cron_interval = 5 # cron函数运行的间隔

    def __init__(self, name="AsyncThread"):
        super(AsyncThread, self).__init__()
        self.setDaemon(True)
        self.setName(name)
        self.task_queue = deque()
        self.cron_func_dict = dict()

    def put_task(self, func, *args, **kw):
        if len(self.task_queue) > self.MAX_TASK_QUEUE:
            logging.error("too many log task, size: %s, max_size: %s",
                           len(self.task_queue), self.MAX_TASK_QUEUE)
            func(*args, **kw)
        else:
            self.task_queue.append([func, args, kw])
    
    def put_cron_func(self, key, func):
        self.cron_func_dict[key] = func

    def run_cron_func(self):
        for key in self.cron_func_dict:
            func = self.cron_func_dict.get(key)
            if func != None:
                self.put_task(func)

    def run(self):
        start_idle_time = time.time()

        while True:
            # queue.Queue默认是block模式
            # 但是deque没有block模式，popleft可能抛出IndexError异常
            try:
                if self.task_queue:
                    func, args, kw = self.task_queue.popleft()
                    func(*args, **kw)
                    start_idle_time = time.time() # 重置 start_idle_time
                else:
                    time.sleep(0.01)
                    idle_time = time.time() - start_idle_time
                    if idle_time >= self.cron_interval:
                        self.run_cron_func()
            except Exception as e:
                exc = traceback.format_exc()
                logging.error("execute failed, %s", exc)
