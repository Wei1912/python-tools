#!/usr/bin/env python
# -*- coding: utf-8 -*-

##################################################
# need package:
#   mysql-connector-python - https://pypi.org/project/mysql-connector-python/
#   xlrd                   - https://pypi.org/project/xlrd/
#
# insert data quickly for MySQL, MariaDB
# read original data from csv, excel
##################################################


import os
import sys
import traceback
import mysql.connector
import xlrd


##################################################
# database connection information
glb_db_host = ''
glb_db_port = 3306
glb_db_database = ''
glb_db_user = ''
glb_db_pwd = ''

# insertion target
glb_db_schema = glb_db_database
glb_db_table = ''

# data file
glb_file_dir = r''
glb_file_name = r''

# options

# file type. 1: text, 2: excel, 0: decide by file extension
glb_file_type = 0
glb_batch_insert_size = 1000

# settings for text file
glb_file_encoding = 'utf-8'   # use utf-8-sig for utf8 with BOM
glb_text_delimiter = ','
glb_text_qualifier = '"'

# settings for excel
glb_excel_sheet_name = ''
##################################################


def retrieve_data_type(schema, table_name, cur):
    sql = 'select column_name, data_type from information_schema.columns where table_schema = '
    sql = sql + "'" + schema + "' and table_name = "
    sql = sql + "'" + table_name + "'"

    cur.execute(sql)
    columns = cur.fetchall()
    types = {}
    for c in columns:
        types[c[0]] = c[1]
    return types


def format_data(fields, row, types, is_excel):
    i = 0
    while i < len(fields):
        if row[i]:
            t = types[fields[i]]
            if t in ['varchar', 'char']:
                row[i] = str(row[i])
            elif t in ['integer', 'int']:
                row[i] = int(row[i])
            elif t in ['real', 'double']:
                row[i] = float(row[i])
            elif t == 'date' and is_excel:
                d = xlrd.xldate.xldate_as_tuple(row[i], 0)
                row[i] = '-'.join(str(x) for x in d[:3])
        if row[i] == '':
            row[i] = None
        i = i + 1
    return row


def read_text_line(line):
    line = line.strip()
    items = []
    chars = []
    quote = False
    for c in line:
        if c == '"':
            quote = not quote
        elif c == "," and not quote:
            items.append(''.join(chars).strip())
            chars.clear()
        else:
            chars.append(c)
    if len(chars) > 0:
        items.append(''.join(chars).strip())
    return items


def read_text_file(file_path, file_encoding):
    headers = None
    data = []
    with open(file_path, 'r', encoding=file_encoding) as f:
        i = 0
        for line in f:
            items = read_text_line(line)
            if i == 0:
                headers = items
            else:
                data.append(items)
            i = i + 1
    return headers, data


def read_excel(file_path, sheet_name):
    wb = xlrd.open_workbook(file_path)
    sheet = wb.sheet_by_name(sheet_name)
    headers = [str(x.value) for x in sheet.row(0)]
    data = [None] * (sheet.nrows - 1)
    for i in range(1, sheet.nrows):
        row = [None] * sheet.ncols
        for j in range(sheet.ncols):
            row[j] = sheet.cell_value(i, j)
        data[i-1] = row
    return headers, data


def get_file_type_from_file_name(file_path):
    root, ext = os.path.splitext(file_path)
    ext = ext[1:]
    if ext in ['csv', 'txt', 'tsv']:
        return 1
    elif ext in ['xls', 'xlsx']:
        return 2
    else:
        print(file_path)
        raise Exception('Unrecognized file type.')


def create_query(schema, table, fields):
    query = 'INSERT INTO `' + schema + '`.`' + table + '` ('
    for f in fields:
        query = query + '`' + f + '`,'
    if query.endswith(','):
        query = query[:-1]
    query = query + ') VALUES (' + ','.join(['%s'] * len(fields)) + ')'
    return query


def process():
    file_path = os.path.join(glb_file_dir, glb_file_name)
    file_type = glb_file_type
    if file_type == 0:
        file_type = get_file_type_from_file_name(file_path)

    # read data from file
    if file_type == 1:
        headers, data = read_text_file(file_path, glb_file_encoding)
    elif file_type == 2:
        headers, data = read_excel(file_path, glb_excel_sheet_name)

    try:
        conn = mysql.connector.connect(host=glb_db_host, port=glb_db_port, database=glb_db_database,
                                       user=glb_db_user, password=glb_db_pwd)
    except Exception:
        print('Connection failed!')
        traceback.print_exc()
        sys.exit(1)
    cur = conn.cursor()

    query = create_query(glb_db_schema, glb_db_table, headers)
    print('query:')
    print(query)
    print()
    types = retrieve_data_type(glb_db_schema, glb_db_table, cur)

    batch_insert_size = glb_batch_insert_size
    batch_insert_data = [None] * batch_insert_size
    batch_insert_cnt = 0
    i = 0
    print('insertion start')
    while i < len(data):
        # read data
        row = data[i]
        if not row or len(row) == 0:
            continue
        row = format_data(headers, row, types, file_type == 2)

        batch_insert_data[batch_insert_cnt] = tuple(row)
        batch_insert_cnt = batch_insert_cnt + 1

        # insert data
        if batch_insert_cnt == batch_insert_size:
            cur.executemany(query, batch_insert_data)
            conn.commit()
            print(str(batch_insert_size) + ' rows inserted.')

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
    print('total: ' + str(len(data)) + ' rows.')


if __name__ == '__main__':
    process()
