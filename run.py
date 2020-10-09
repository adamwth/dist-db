import psycopg2
import psycopg2.errorcodes
import time
import logging
import random
import sys
import argparse

logging.basicConfig(level=logging.DEBUG)


def readAndExecSql(conn, file):
    with conn.cursor() as cur:
        try:
            cur.execute(file.read())
            print(cur.fetchall())
        except psycopg2.Error as e:
            logging.debug(e)
    conn.commit()


def main():
    # Usage: python3 setup.py <file> <hostNum> <db>
    # Example: python3 setup.py create-tables.sql 2
    # if hostNum not specified, use default host 2 (i.e. xcnc2)
    parser = argparse.ArgumentParser()
    parser.add_argument("file", metavar="F",
                        type=argparse.FileType('r'),
                        help='File with SQL to be executed')
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
    user = 'admin'  # use admin so we don't have to grant privileges manually
    database = args.database
    conn = psycopg2.connect(host=host,
                            port=port,
                            user=user,
                            database=database)
    readAndExecSql(conn, args.file)
    args.file.close()


if __name__ == '__main__':
    main()
