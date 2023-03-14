from __future__ import absolute_import
import sys
sys.path.insert(0, "./")
import sqlite_rw
import threading
import time
import sqlite3
import traceback
import random

def get_db_file():
    return "./test_write.db"

def get_read_file():
    return "./test_read.db"

def init_user_bench_table():
    with sqlite_rw.TableManager(get_db_file(), "user_bench", read_db_path = get_read_file()) as manager:
        manager.add_column("name", "text", "")
        manager.add_column("age", "int", 0)

def get_table(timeout = 5, default_read_type = "read"):
    return sqlite_rw.SqliteTable(get_db_file(), "user_bench", 
                                 read_db_path = get_read_file(), timeout = timeout,
                                 default_read_type = default_read_type)

def start_new_thread(target) -> threading.Thread:
    t = threading.Thread(target = target)
    t.start()
    return t

init_user_bench_table()

def rand_str(length):
    v = ""
    a = ord('A')
    b = ord('Z')
    for i in range(length):
        v += chr(random.randint(a, b))
    return v

def test_insert_large():
    """插入100万数据"""
    print("\n\n=== test_insert_large")
    table = get_table()

    max_count = 1000
    start_time = time.time()

    for i in range(1000):
        count = table.count_from_write()
        if count >= max_count:
            cost_time = time.time() - start_time
            print("插入数据: ", count)
            print("耗时: %.4fs" % cost_time)
            print("QPS: %d" % (count/cost_time))
            return
        try:
            # 使用事务提升insert速度
            with table.transaction():
                for j in range(100):
                    table.insert(name = "name-" + rand_str(30), age = random.randint(10,50))
        except sqlite3.OperationalError:
            traceback.print_exc()
