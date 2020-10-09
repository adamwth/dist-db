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
        except psycopg2.Error as e:
            logging.debug(e)
    conn.commit()


def main():
    # Usage: python3 setup.py <hostNum> <file>
    # Example: python3 setup.py 2
    # if arg not specified, use default host 2
    parser = argparse.ArgumentParser()
    parser.add_argument("hostNum", metavar='H',
                        type=int,
                        help='Host number e.g. 2 for xcnc2'
                        )
    parser.add_argument("file", metavar="F",
                        type=argparse.FileType('r'),
                        help='File with SQL to be executed')
    args = parser.parse_args()

    host = f'xcnc{args.hostNum}.comp.nus.edu.sg'
    port = '26259'
    user = 'admin'  # use admin so we don't have to grant privileges manually
    database = 'project'
    conn = psycopg2.connect(host=host,
                            port=port,
                            user=user,
                            database=database)
    readAndExecSql(conn, args.file)
    args.file.close()


if __name__ == '__main__':
    main()
