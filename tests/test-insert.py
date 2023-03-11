from __future__ import absolute_import
import sys
sys.path.append("./")
import sqlite_rw

def get_db_file():
    return "./test_write.db"

def init_user_table():
    with sqlite_rw.TableManager(get_db_file(), "user", read_db_path = "./test_read.db") as manager:
        manager.add_column("name", "text", "")
        manager.add_column("age", "int", 0)

def test_insert():
    init_user_table()
    db = sqlite_rw.DBWrapper(get_db_file(), "user", read_db_path = "./test_read.db")
    db.insert(name = "test", age = 20)
    rows = list(db.select_from_read(where = dict(name = "test")))
    for record in rows:
        print(record)
    assert len(rows) == 1


if __name__ == "__main__":
    test_insert()