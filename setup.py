import csv
import psycopg2
import psycopg2.errorcodes
import time
import logging
import random
import sys
import argparse

logging.basicConfig(level=logging.DEBUG)


def execSqlFromFile(conn, file):
    execSqlTransaction(conn, file.read())


def execSqlTransaction(conn, query: str):
    try:
        with conn.cursor() as cur:
            cur.execute(query)
        conn.commit()
    except psycopg2.Error as e:
        logging.debug(e)
        conn.rollback()


def createTables(conn):
    with open('create-tables.sql', 'r') as f:
        execSqlFromFile(conn, f)


def main():
    # Usage: python3 setup.py <hostNum> <db>
    # Example: python3 setup.py create-tables.sql 2
    # if hostNum not specified, use default host 2 (i.e. xcnc2)
    parser = argparse.ArgumentParser()
    # parser.add_argument("file", metavar="F",
    #                     type=argparse.FileType('r'),
    #                     help='File with SQL to be executed')
    parser.add_argument("-hn", '--hostNum',
                        type=int, default=2,
                        help='Host number e.g. 2 for xcnc2. Default is xcnc2.'
                        )
    parser.add_argument("-db", '--database',
                        type=str, default="project",
                        help='Database name'
                        )
    args = parser.parse_args()

    host = f'xcnc{args.hostNum}.comp.nus.edu.sg'
    port = '26259'
    user = 'root'  # use admin so we don't have to grant privileges manually
    database = args.database
    conn = psycopg2.connect(host=host,
                            port=port,
                            user=user,
                            database=database)
    createTables(conn)

    cur = conn.cursor()
    tablesOrdered = [
        {
            'tableName': 'warehouse',
            'fileName': 'warehouse'
        },
        {
            'tableName': 'district',
            'fileName': 'district'
        },
        {
            'tableName': 'customer',
            'fileName': 'customer'
        },
        {
            'tableName': 'order',
            'fileName': 'order'
        },
        {
            'tableName': 'item',
            'fileName': 'item'
        },
        {
            'tableName': 'orderLine',
            'fileName': 'order-line'
        },
        {
            'tableName': 'stock',
            'fileName': 'stock'
        }
    ]

    try:
        for table in tablesOrdered:
            print(table)
            path = f'data-files/{table["fileName"]}.csv'
            with open(path, 'r') as f:
                reader = csv.reader(f)
                for row in reader:
                    numValuesArgStr = (len(row) * "%s, ")[:-2]
                    cur.execute(
                        f'INSERT INTO {table["tableName"]} VALUES ({numValuesArgStr})', row)
        conn.commit()
    except psycopg2.Error as e:
        logging.debug(e)
        conn.rollback()

    args.file.close()


if __name__ == '__main__':
    main()
