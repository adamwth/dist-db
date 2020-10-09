import psycopg2
import psycopg2.errorcodes
import time
import logging
import random
import sys

logging.basicConfig(level=logging.DEBUG)


def readAndExecSql(conn, filename):
    with open(filename, 'r') as fd:
        sqlFile = fd.read()

    sqlCommands = sqlFile.split(';')

    with conn.cursor() as cur:
        for command in sqlCommands:
            # cur.execute(command)
            try:
                cur.execute(command)
            except psycopg2.Error as e:
                logging.debug(e)
    conn.commit()


def main():
    # Usage: python3 setup.py <hostNum>
    # Example: python3 setup.py 2
    # if arg not specified, use default host 2
    hostNum = 2 if len(sys.argv) <= 1 else sys.argv[1]
    host = f'xcnc{hostNum}.comp.nus.edu.sg'
    port = '26259'
    user = 'admin'  # use admin so we don't have to grant privileges manually
    database = 'bank'
    conn = psycopg2.connect(host=host,
                            port=port,
                            user=user,
                            database=database)
    readAndExecSql(conn, 'test.sql')


if __name__ == '__main__':
    main()
