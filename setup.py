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


def loadData(conn):
    conn.set_session(autocommit=True)
    with open('load-data.sql', 'r') as f:
        execSqlFromFile(conn, f)
    conn.set_session(autocommit=False)


def dropTables(conn):
    with open('drop-tables.sql', 'r') as f:
        execSqlFromFile(conn, f)


def connectDb(hostNum, database):
    host = f'xcnc{hostNum}.comp.nus.edu.sg'
    port = '26259'
    user = 'root'  # use root so we don't have to grant privileges manually
    conn = psycopg2.connect(host=host,
                            port=port,
                            user=user,
                            database=database)
    return conn


def setupParser():
    # Usage: python3 setup.py <hostNum> <db> [-d]
    # Example: python3 setup.py 2
    # if hostNum not specified, use default host 2 (i.e. xcnc2)
    parser = argparse.ArgumentParser()
    parser.add_argument("-hn", '--hostNum',
                        type=int, default=2,
                        help='Host number e.g. 2 for xcnc2. Default is xcnc2.'
                        )
    parser.add_argument("-db", '--database',
                        type=str, default="project",
                        help='Database name. Default is "project"'
                        )
    parser.add_argument("-d", action="store_true",
                        help="Set flag to only drop tables.")
    return parser


def main():
    # Setup parser arguments
    parser = setupParser()
    args = parser.parse_args()

    # Connect to DB
    conn = connectDb(args.hostNum, args.database)

    # If -d flag was specified, drop tables only and return
    dropTables(conn)
    if args.d:
        return
    createTables(conn)
    loadData(conn)


if __name__ == '__main__':
    main()
