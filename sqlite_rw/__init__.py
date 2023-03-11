# encoding=utf-8
import sqlite3
import web
import logging
import json
import threading
import time
import traceback
from collections import deque

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s|%(levelname)s|%(filename)s:%(lineno)d|%(message)s')


class AsyncThread(threading.Thread):
    
    MAX_TASK_QUEUE = 200
    lock = threading.RLock()

    def __init__(self, name="AsyncThread"):
        super(AsyncThread, self).__init__()
        self.setDaemon(True)
        self.setName(name)
        self.task_queue = deque()

    def put_task(self, func, *args, **kw):
        if len(self.task_queue) > self.MAX_TASK_QUEUE:
            logging.error("too many log task, size: %s, max_size: %s",
                           len(self.task_queue), self.MAX_TASK_QUEUE)
            func(*args, **kw)
        else:
            self.task_queue.append([func, args, kw])

    def run(self):
        while True:
            # queue.Queue默认是block模式
            # 但是deque没有block模式，popleft可能抛出IndexError异常
            try:
                if self.task_queue:
                    func, args, kw = self.task_queue.popleft()
                    func(*args, **kw)
                else:
                    time.sleep(0.01)
            except Exception as e:
                exc = traceback.format_exc()
                logging.error("execute failed, %s", exc)

_async_thread = AsyncThread()
_async_thread.daemon = True
_async_thread.start()

def async_func_deco():
    """同步调用转化成异步调用的装饰器"""
    def deco(func):
        def handle(*args, **kw):
            _async_thread.put_task(func, *args, **kw)
        return handle
    return deco

class SqliteTableManager:
    """检查数据库字段，如果不存在就自动创建"""

    def __init__(self, filename, tablename, pkName=None, pkType=None, no_pk=False, read_db_path=""):
        self.filename = filename
        self.tablename = tablename
        self.read_db = None

        if read_db_path != "":
            self.read_db = sqlite3.connect(read_db_path)
        self.db = sqlite3.connect(filename)

        for db in self._get_db_list():
            find_sql = "SELECT * FROM sqlite_master WHERE name = %r;" % tablename
            result = self.do_execute(db, find_sql, silent=True)
            if len(result) > 0:
                continue

            if no_pk:
                # 没有主键，创建一个占位符
                sql = "CREATE TABLE IF NOT EXISTS `%s` (_id int);" % tablename
            elif pkName is None:
                # 只有integer允许AUTOINCREMENT
                sql = "CREATE TABLE IF NOT EXISTS `%s` (id integer primary key autoincrement);" % tablename
            else:
                # sqlite允许主键重复，允许空值
                sql = "CREATE TABLE IF NOT EXISTS `%s` (`%s` %s primary key);" % (
                    tablename, pkName, pkType)
            self.do_execute(db, sql)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def _get_db_list(self):
        if self.read_db == None:
            return [self.db]
        return [self.db, self.read_db]

    def execute(self, sql, silent=False):
        return self.do_execute(self.db, sql, silent)

    def execute_write_and_read(self, sql, silent=False):
        result = self.do_execute(self.db, sql, silent)
        if self.read_db != None:
            self.do_execute(self.read_db, sql, silent)
        return result

    def do_execute(self, db, sql, silent=False):
        cursorobj = db.cursor()
        try:
            if not silent:
                logging.info(sql)
            cursorobj.execute(sql)
            kv_result = []
            result = cursorobj.fetchall()
            for single in result:
                resultMap = {}
                for i, desc in enumerate(cursorobj.description):
                    name = desc[0]
                    resultMap[name] = single[i]
                kv_result.append(resultMap)
            db.commit()
            return kv_result
        except Exception:
            raise

    def escape(self, strval):
        strval = strval.replace("'", "''")
        return "'%s'" % strval

    def define_column(self, colname, coltype,
                   default_value=None, not_null=False):
        """添加字段，如果已经存在则跳过，名称相同类型不同抛出异常"""
        sql = "ALTER TABLE `%s` ADD COLUMN `%s` %s" % (
            self.tablename, colname, coltype)

        for db in self._get_db_list():
            # MySQL 使用 DESC [表名]
            columns = self.do_execute(
                db, "pragma table_info('%s')" % self.tablename, silent=True)
            # print(columns.description)
            # description结构
            is_col_exists = False

            for column in columns:
                name = column["name"]
                type = column["type"]
                if name == colname:
                    # 已经存在
                    is_col_exists = True
                
            if is_col_exists:
                continue

            if default_value != None:
                if isinstance(default_value, str):
                    default_value = self.escape(default_value)
                sql += " DEFAULT %s" % default_value
            if not_null:
                sql += " NOT NULL"
            self.do_execute(db, sql)
    
    add_column = define_column

    def add_index(self, colname, is_unique=False):
        # sqlite的索引和table是一个级别的schema
        if isinstance(colname, list):
            idx_name = "idx_" + self.tablename
            for name in colname:
                idx_name += "_" + name
            colname_str = ",".join(colname)
            sql = "CREATE INDEX IF NOT EXISTS %s ON `%s` (%s)" % (
                idx_name, self.tablename, colname_str)
        else:
            sql = "CREATE INDEX IF NOT EXISTS idx_%s_%s ON `%s` (`%s`)" % (
                self.tablename, colname, self.tablename, colname)
        try:
            self.execute_write_and_read(sql)
        except Exception as e:
            logging.error("sql:%s, err:%s", sql, e)

    def drop_index(self, col_name):
        sql = "DROP INDEX idx_%s_%s" % (self.tablename, col_name)
        try:
            self.execute_write_and_read(sql)
        except Exception as e:
            logging.error("sql:%s, err:%s", sql, e)

    def drop_column(self, colname):
        # sql = "ALTER TABLE `%s` DROP COLUMN `%s`" % (self.tablename, colname)
        # sqlite不支持 DROP COLUMN 得使用中间表
        # TODO
        pass

    def generate_migrate_sql(self, dropped_names):
        """生成迁移字段的SQL（本质上是迁移）"""
        columns = self.execute("pragma table_info('%s')" %
                               self.tablename, silent=True)
        new_names = []
        old_names = []
        for column in columns:
            name = column["name"]
            type = column["type"]
            old_names.append(name)
            if name not in dropped_names:
                new_names.append(name)
        # step1 = "ALTER TABLE %s RENAME TO backup_table;" % (self.tablename)
        step2 = "INSERT INTO %s (%s) \nSELECT %s FROM backup_table;" % (
                self.tablename,
                ",".join(new_names),
                ",".join(old_names)
        )
        return step2

    def close(self):
        self.db.close()
        if self.read_db != None:
            self.read_db.close()


class SqliteTable:
    """基于web.db的装饰器
    SqliteDB是全局唯一的，它的底层使用了连接池技术，每个线程都有独立的sqlite连接
    """

    def __init__(self, dbpath, tablename, read_db_path="", timeout = 5, default_read_type = "read"):
        assert read_db_path != "", "read_db_path is empty"
        self.tablename = tablename
        self.dbpath = dbpath
        self.read_db_path = read_db_path
        self.binlog_table = "binlog"
        # SqliteDB 内部使用了threadlocal来实现，是线程安全的，使用全局单实例即可

        self.db = web.db.SqliteDB(db=dbpath, timeout = timeout)
        self.read_db = web.db.SqliteDB(db=read_db_path, timeout = timeout)

        if default_read_type == "write":
            self.default_db = self.db
        else:
            self.default_db = self.read_db

        self.init_binlog_table(dbpath)

    def copy_to_read(self):
        with AsyncThread.lock:
            try:
                db = web.db.SqliteDB(db = self.dbpath, timeout = 1)
                read_db = web.db.SqliteDB(db = self.read_db_path, timeout = 1)
                records = list(db.select(self.binlog_table, limit = 10, order = "id"))

                for record in records:
                    op_type = record.op_type
                    data_str = record.data
                    tablename = record.table_name
                    data = json.loads(data_str)

                    if op_type == "insert":
                        read_db.insert(tablename, **data)
                    elif op_type == "update":
                        data_id = data.get("id")
                        read_db.update(tablename, where = dict(id=data_id), **data)
                    elif op_type == "delete_by_ids":
                        read_db.delete(tablename, where = "id in $ids", vars = dict(ids = data))
                    else:
                        raise Exception("unknown op_type:%s" % op_type)
                    
                    db.delete(self.binlog_table, where=dict(id=record.id))
            except sqlite3.OperationalError as e:
                logging.error("copy_to_read failed, err:%s", e)

    @async_func_deco()
    def copy_to_read_async(self):
        return self.copy_to_read()

    def init_binlog_table(self, db_file):
        with SqliteTableManager(db_file, "binlog") as manager:
            manager.add_column("table_name", "text", "")
            manager.add_column("op_type", "text", "")
            manager.add_column("data", "text", "")

    def _insert_binlog(self, op_type, data):
        self.db.insert(self.binlog_table, table_name=self.tablename,
                       op_type=op_type,
                       data=json.dumps(data))

    def insert(self, *args, **kw):
        with self.db.transaction():
            insert_id = self.db.insert(self.tablename, *args, **kw)
            insert_value = self.db.select(
                self.tablename, where=dict(id=insert_id)).first()
            self._insert_binlog(op_type="insert", data=insert_value)
            self.copy_to_read_async()
            return insert_id

    def select(self, *args, **kw):
        return self.default_db.select(self.tablename, *args, **kw)

    def select_from_write(self, *args, **kw):
        return self.db.select(self.tablename, *args, **kw)

    def select_first(self, *args, **kw):
        return self.default_db.select(self.tablename, *args, **kw).first()
    
    def select_first_from_write(self, *args, **kw):
        return self.db.select(self.tablename, *args, **kw).first()

    def query(self, *args, **kw):
        return self.default_db.query(*args, **kw)
    
    def query_from_write(self, *args, **kw):
        return self.db.query(*args, **kw)

    def _count(self, db, where=None, sql=None, vars=None):
        if sql is None:
            if isinstance(where, dict):
                return self.select_first(what="COUNT(1) AS amount", where=where).amount
            else:
                sql = "SELECT COUNT(1) AS amount FROM %s" % self.tablename
                if where:
                    sql += " WHERE %s" % where
        return db.query(sql, vars=vars).first().amount

    def count(self, where=None, sql=None, vars=None):
        return self._count(self.default_db, where, sql, vars)
    
    def count_from_write(self, where = None, sql = None, vars = None):
        return self._count(self.db, where, sql, vars)

    def update(self, where, vars=None, _test=False, **values):
        with self.db.transaction():
            ids_results = self.db.select(self.tablename, what="id", where = where, vars = vars, _test = _test)
            ids = list(map(lambda x:x.id, ids_results))
            if len(ids) == 0:
                return
            update_result = self.db.update(self.tablename, where, vars, _test, **values)
            new_records = self.db.select(self.tablename, where = "id in $ids", vars = dict(ids = ids))
            for item in new_records:
                self._insert_binlog(op_type="update", data = item)
            return update_result

    def delete(self, *args, **kw):
        with self.db.transaction():
            ids_results = self.db.select(self.tablename, what="id", *args, **kw)
            ids = list(map(lambda x:x.id, ids_results))
            if len(ids) == 0:
                return
            self._insert_binlog(op_type="delete_by_ids", data=ids)
            return self.db.delete(self.tablename, where="id in $ids", vars=dict(ids=ids))


TableManager = SqliteTableManager
Table = SqliteTable