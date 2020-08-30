#!/usr/bin/env python
# -*- coding: utf-8 -*-

##################################################
# need package:
#   mysql-connector-python - https://pypi.org/project/mysql-connector-python/
#
# generate data then insert into MySQL database
##################################################

import sys
import traceback
import datetime
import random
import string
import mysql.connector


##################################################
# database connection information
glb_db_host = ''
glb_db_port = 3306
glb_db_database = ''
glb_db_user = ''
glb_db_pwd = ''

# insertion target
glb_db_table = 'test1'

glb_row_number = 1000 * 1000 * 2
glb_col_number = 20
glb_vchar_length = 5
glb_batch_insert_size = 5000
##################################################

CHARS = string.ascii_letters + string.digits


def generate_columns():
    """
    list of (column name, data type)
    fixed: id, ts (timestamp)
    non-fixed: COLxx, number given by glb_col_number

    return list of columns without id
    """
    columns = [('ts', 'datetime')]
    zfill_size = len(str(glb_col_number))
    for i in range(1, glb_col_number + 1):
        col_name = 'COL' + str(i).zfill(zfill_size)
        columns.append((col_name, "varchar({0})".format(glb_vchar_length)))
    return columns


def create_table(conn, cur, columns):
    """
    create table, table name given by variable glb_db_table
    no return
    """
    sql = "DROP TABLE IF EXISTS `{0}`.`{1}` CASCADE;".format(glb_db_database, glb_db_table)
    print(sql)
    cur.execute(sql)
    conn.commit()
    print("Dropped table {0}".format(glb_db_table))

    sql = ['CREATE TABLE `', glb_db_database, '`.`', glb_db_table, '` (`id` SERIAL, ']
    length = len(columns)
    for i in range(length):
        c = columns[i]
        sql.append("`{0}` {1}".format(c[0], c[1]))
        if i < length - 1:
            sql.append(', ')
    sql.append(')')

    sql = ''.join(sql)
    print('sql for creating table:')
    print(sql)
    cur.execute(sql)
    conn.commit()
    print("table {0} was created.".format(glb_db_table))


def generate_insert_query(columns):
    query = 'INSERT INTO `' + glb_db_database + '`.`' + glb_db_table + '` ('
    for c in columns:
        query = query + '`' + c[0] + '`,'
    if query.endswith(','):
        query = query[:-1]
    query = query + ') VALUES (' + ','.join(['%s'] * len(columns)) + ')'
    return query


def generate_one_row(columns):
    length = len(columns)
    row = [None] * length
    for i in range(length):
        c = columns[i]
        data_type = c[1].lower()
        if data_type == 'datetime':
            row[i] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        else:
            row[i] = ''.join(random.choice(CHARS) for _ in range(glb_vchar_length))
    return row


def process():
    print("start")
    try:
        conn = mysql.connector.connect(host=glb_db_host,
                                       port=glb_db_port,
                                       database=glb_db_database,
                                       user=glb_db_user,
                                       password=glb_db_pwd)
    except Exception:
        print('Connection failed!')
        traceback.print_exc()
        sys.exit(1)
    print("connected to " + glb_db_host)
    cur = conn.cursor()

    columns = generate_columns()
    print('columns:')
    print(columns)

    # create table
    create_table(conn, cur, columns)

    # generate sql for inserting data
    query = generate_insert_query(columns)
    print('query:')
    print(query)
    print()

    # insert data
    batch_insert_size = glb_batch_insert_size
    batch_insert_data = [None] * batch_insert_size
    batch_insert_cnt = 0
    print('insertion start')
    i = 0
    while i < glb_row_number:
        batch_insert_data[batch_insert_cnt] = tuple(generate_one_row(columns))
        batch_insert_cnt = batch_insert_cnt + 1

        # insert data
        if batch_insert_cnt == batch_insert_size:
            cur.executemany(query, batch_insert_data)
            conn.commit()
            print(str(batch_insert_size) + ' rows inserted. total: ' + str(i))

            batch_insert_data = [None] * batch_insert_size
            batch_insert_cnt = 0
        i = i + 1

    if batch_insert_cnt > 0:
        cur.executemany(query, batch_insert_data[:batch_insert_cnt])
        conn.commit()
        print(str(batch_insert_cnt) + ' rows inserted.')

    cur.close()
    conn.close()

    print('insertion finished')
    print('total: ' + str(glb_row_number) + ' rows.')


if __name__ == '__main__':
    process()
