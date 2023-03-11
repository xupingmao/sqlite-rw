# encoding=utf-8
import sys
import sqlite3
import web
import logging
import json

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s|%(levelname)s|%(filename)s:%(lineno)d|%(message)s')

class SqliteTableManager:
    """检查数据库字段，如果不存在就自动创建"""
    def __init__(self, filename, tablename, pkName=None, pkType=None, no_pk=False, read_db_path = ""):
        self.filename = filename
        self.tablename = tablename
        self.read_db = None

        if read_db_path != "":
            self.read_db = sqlite3.connect(read_db_path)
        self.db = sqlite3.connect(filename)
        if no_pk:
            # 没有主键，创建一个占位符
            sql = "CREATE TABLE IF NOT EXISTS `%s` (_id int);" % tablename
        elif pkName is None:
            # 只有integer允许AUTOINCREMENT
            sql = "CREATE TABLE IF NOT EXISTS `%s` (id integer primary key autoincrement);" % tablename
        else:
            # sqlite允许主键重复，允许空值
            sql = "CREATE TABLE IF NOT EXISTS `%s` (`%s` %s primary key);" % (tablename, pkName, pkType)
        self.execute_write_and_read(sql)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def _get_db_list(self):
        if self.read_db == None:
            return [self.db]
        return [self.db, self.read_db]

    def execute(self, sql, silent = False):
        return self.do_execute(self.db, sql, silent)
    
    def execute_write_and_read(self, sql, silent = False):
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

    def add_column(self, colname, coltype, 
            default_value = None, not_null = False):
        """添加字段，如果已经存在则跳过，名称相同类型不同抛出异常"""
        sql = "ALTER TABLE `%s` ADD COLUMN `%s` %s" % (self.tablename, colname, coltype)

        for db in self._get_db_list():
            # MySQL 使用 DESC [表名]
            columns = self.do_execute(db, "pragma table_info('%s')" % self.tablename, silent=True)
            # print(columns.description)
            # description结构
            for column in columns:
                name = column["name"]
                type = column["type"]
                if name == colname:
                    # 已经存在
                    return
            if default_value != None:
                if isinstance(default_value, str):
                    default_value = self.escape(default_value)
                sql += " DEFAULT %s" % default_value
            if not_null:
                sql += " NOT NULL"
            self.do_execute(db, sql)

    def add_index(self, colname, is_unique = False):
        # sqlite的索引和table是一个级别的schema
        if isinstance(colname, list):
            idx_name = "idx_" + self.tablename
            for name in colname:
                idx_name += "_" + name
            colname_str = ",".join(colname)
            sql = "CREATE INDEX IF NOT EXISTS %s ON `%s` (%s)" % (idx_name, self.tablename, colname_str)
        else:
            sql = "CREATE INDEX IF NOT EXISTS idx_%s_%s ON `%s` (`%s`)" % (self.tablename, colname, self.tablename, colname)
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
        columns = self.execute("pragma table_info('%s')" % self.tablename, silent=True)
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


TableManager = SqliteTableManager


class DBWrapper:
    """基于web.db的装饰器
    SqliteDB是全局唯一的，它的底层使用了连接池技术，每个线程都有独立的sqlite连接
    """

    def __init__(self, dbpath, tablename, read_db_path = ""):
        assert read_db_path != "", "read_db_path is empty"
        self.tablename = tablename
        self.dbpath = dbpath
        self.binlog_table = "binlog"
        # SqliteDB 内部使用了threadlocal来实现，是线程安全的，使用全局单实例即可

        self.db = web.db.SqliteDB(db = dbpath)
        self.read_db = web.db.SqliteDB(db = read_db_path)

        self.init_binlog_table(dbpath)
    
    def copy_to_read_async(self):
        for record in self.db.select(self.binlog_table, limit = 10, order = "id"):
            op_type = record.op_type
            data_str = record.data
            tablename = record.table_name
            data = json.loads(data_str)

            if op_type == "insert":
                self.read_db.insert(tablename, **data)
                self.db.delete(self.binlog_table, where = dict(id = record.id))

    
    def init_binlog_table(self, db_file):
        with TableManager(db_file, "binlog") as manager:
            manager.add_column("table_name", "text", "")
            manager.add_column("op_type", "text", "")
            manager.add_column("data", "text", "")

    def insert(self, *args, **kw):
        with self.db.transaction():
            insert_id = self.db.insert(self.tablename, *args, **kw)
            insert_value = list(self.db.select(self.tablename, where = dict(id = insert_id)))[0]
            self.db.insert(self.binlog_table, table_name = self.tablename, 
                op_type = "insert", data = json.dumps(insert_value))
            self.copy_to_read_async()
            return insert_id

    def select(self, *args, **kw):
        return self.db.select(self.tablename, *args, **kw)

    def select_first(self, *args, **kw):
        return self.db.select(self.tablename, *args, **kw).first()

    def query(self, *args, **kw):
        return self.db.query(*args, **kw)

    def count(self, where=None, sql = None, vars = None):
        if sqlite3 is None:
            return 0
        if sql is None:
            if isinstance(where, dict):
                return self.select_first(what = "COUNT(1) AS amount", where = where).amount
            else:
                sql = "SELECT COUNT(1) AS amount FROM %s" % self.tablename
                if where:
                    sql += " WHERE %s" % where
        return self.db.query(sql, vars = vars).first().amount

    def update(self, *args, **kw):
        return self.db.update(self.tablename, *args, **kw)

    def delete(self, *args, **kw):
        return self.db.delete(self.tablename, *args, **kw)
