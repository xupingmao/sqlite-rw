from __future__ import absolute_import
import sys
sys.path.append("./")
import sqlite_rw
import threading
import time
import sqlite3
import traceback
import termcolor
from sqlite_rw import _async_thread

def get_db_file():
    return "./test_write.db"

def get_read_file():
    return "./test_read.db"

def init_user_table():
    with sqlite_rw.TableManager(get_db_file(), "user", read_db_path = get_read_file()) as manager:
        manager.add_column("name", "text", "")
        manager.add_column("age", "int", 0)

def get_table(timeout = 5, default_read_type = "read"):
    return sqlite_rw.SqliteTable(get_db_file(), "user", 
                                 read_db_path = get_read_file(), timeout = timeout,
                                 default_read_type = default_read_type)

def start_new_thread(target) -> threading.Thread:
    t = threading.Thread(target = target)
    t.start()
    return t

def test_insert():
    print("\n\n=== test_insert")
    db = sqlite_rw.SqliteTable(get_db_file(), "user", read_db_path = get_read_file())

    db.copy_to_read()

    db.delete(where = dict(name = "test"))
    db.insert(name = "test", age = 20)

    # 强制完成同步
    db.copy_to_read()

    rows = list(db.select(where = dict(name = "test")))
    for record in rows:
        print(record)
    assert len(rows) == 1

def test_update():
    print("\n\n=== test_update")
    table = get_table()
    table.insert(name = "test", age = 20)
    table.update(where = dict(name = "test"), age = 23)
    # 强制同步
    table.copy_to_read()
    record = table.select_first(where = dict(name = "test"))
    assert record.age == 23

class Result:

    def __init__(self) -> None:
        self.is_locked = False
        self.is_executed = False
        self.is_read_locked = False

def print_start(msg):
    print(termcolor.colored(">>> " + msg, "cyan"))

def print_end(msg):
    print(termcolor.colored("<<< " + msg, "cyan"))

def run_test_read_lock_base(read_by_write = True):
    # sqlite锁默认5秒超时
    db = get_table()
    db.delete(where = "name like $name", vars = dict(name = "test%"))
    db.insert(name = "test-1", age = 10)
    db.insert(name = "test-2", age = 20)

    db.copy_to_read()

    result = Result()
    result.is_read_locked = True

    def read_and_lock():
        if read_by_write:
            default_read_type = "write"
        else:
            default_read_type = "read"

        read_db = get_table(default_read_type = default_read_type)

        with read_db.default_db.transaction():
            print(">>> begin transaction for read")
            result = read_db.select(where = "name like $name", vars = dict(name = "test%"))
            has_sleep = False
            for item in result:
                print("read_item:", item)
                if not has_sleep:
                    time.sleep(2)
                    has_sleep = True
        print("<<< commit transaction for read")
    
    def write_and_lock():
        time.sleep(0.5) # 等待读操作先执行
        print(">>> begin transaction for write")
        try:
            write_db = get_table(timeout = 1)
            write_db.insert(name = "test-3", age = 25)
        except sqlite3.OperationalError as e:
            traceback.print_exc()
            if "database is locked" in str(e):
                result.is_locked = True
        print("<<< commit transaction for write")
    
    def read_no_wait():
        time.sleep(0.5) # 等待读操作先执行
        print_start("begin transaction for read_no_wait")
        read_db = get_table(timeout=1)
        for item in read_db.select(where = "name like $name", vars = dict(name = "test%")):
            print("read_no_wait:", item)
        print_end("commit transaction for read_no_wait")
        result.is_read_locked = False

    t1 = start_new_thread(read_and_lock)
    t2 = start_new_thread(write_and_lock)
    t3 = start_new_thread(read_no_wait)
    t1.join()
    t2.join()
    t3.join()

    assert result.is_read_locked == False # read之间是可以并发的
    return result


def test_read_locked():
    print("\n\n=== test_read_locked")
    result = run_test_read_lock_base(read_by_write=True)
    assert result.is_locked == True

def test_read_nolock():
    print("\n\n=== test_read_nolock")
    result = run_test_read_lock_base(read_by_write=False)
    assert result.is_locked == False


def test_copy_cron():
    print("\n\n=== test_copy_cron")
    result = Result()

    def test_cron_func():
        result.is_executed = True

    _async_thread.cron_interval = 0.1
    _async_thread.put_cron_func("test", test_cron_func)
    time.sleep(2)

    assert result.is_executed

    

init_user_table()

