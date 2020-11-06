import math
import sys
from datetime import timedelta, datetime
import transaction
import argparse
import psycopg2
from psycopg2.errors import SerializationFailure
import random
import time
import os

TXN_ID = {
    "NEW_ORDER": "N",
    "PAYMENT": "P",
    "DELIVERY": "D",
    "ORDER_STATUS": "O",
    "STOCK_LEVEL": "S",
    "POPULAR_ITEM": "I",
    "TOP_BALANCE": "T",
    "RELATED_CUSTOMER": "R",
}


def get_percentile(data, percentile):
    size = len(data)
    return data[int(math.ceil((size * percentile) / 100)) - 1]


class MetricsManager:
    def __init__(self):
        # Keeps track of time taken for each transaction in milliseconds
        self.transaction_timings = []
        self.total_time = timedelta(0)

    # Takes in a timedelta for a transaction
    def add(self, delta):
        self.transaction_timings.append(delta / timedelta(milliseconds=1))

    def add_total_time(self, delta):
        self.total_time = delta

    def output_metrics(self):
        total_transactions = len(self.transaction_timings)
        sorted_transaction_timings = sorted(self.transaction_timings)
        total_transaction_timings = sum(sorted_transaction_timings)
        sys.stderr.write("Number of executed transactions: {}\n".format(total_transactions))
        sys.stderr.write("Total transaction execution time (in seconds): {}\n".format(self.total_time.total_seconds()))
        sys.stderr.write(
            "Transaction throughput (transactions / s): {}\n".format(
                total_transactions / (self.total_time.total_seconds())))
        sys.stderr.write(
            "Average transaction latency (in milliseconds): {}\n".format(
                total_transaction_timings / total_transactions))
        sys.stderr.write(
            "Median transaction latency (in milliseconds): {}\n".format(get_percentile(sorted_transaction_timings, 50)))
        sys.stderr.write("95th percentile transaction latency (in milliseconds): {}\n".format(
            get_percentile(sorted_transaction_timings, 95)))
        sys.stderr.write("99th percentile transaction latency (in milliseconds): {}\n".format(
            get_percentile(sorted_transaction_timings, 99)))

    def write_metrics(self, filename):
        metrics = []
        total_transactions = len(self.transaction_timings)
        sorted_transaction_timings = sorted(self.transaction_timings)
        total_transaction_timings = sum(sorted_transaction_timings)
        metrics.append(str(total_transactions))
        metrics.append(str(self.total_time.total_seconds()))
        metrics.append(str(total_transactions / (self.total_time.total_seconds())))
        metrics.append(str(total_transaction_timings / total_transactions))
        metrics.append(str(get_percentile(sorted_transaction_timings, 50)))
        metrics.append(str(get_percentile(sorted_transaction_timings, 95)))
        metrics.append(str(get_percentile(sorted_transaction_timings, 99)))
        with open(filename, "w") as f:
            f.write(",".join(metrics))


def setup_transactions(file, conn):
    transactions = []
    line = file.readline()
    while line:
        args = line.rstrip('\n').split(',')
        identifier = args[0]
        if identifier == TXN_ID["NEW_ORDER"]:
            inputs = args[1:]
            # Adds each item as a nested list
            num_items = int(args[-1])
            for i in range(0, num_items):
                item_line = file.readline()
                inputs.append(item_line.rstrip('\n').split(','))
            transactions.append(transaction.NewOrderTransaction(conn, inputs))
        elif identifier == TXN_ID["PAYMENT"]:
            transactions.append(transaction.PaymentTransaction(conn, args[1:]))
        elif identifier == TXN_ID["DELIVERY"]:
            transactions.append(transaction.DeliveryTransaction(conn, args[1:]))
        elif identifier == TXN_ID["ORDER_STATUS"]:
            transactions.append(transaction.OrderStatusTransaction(conn, args[1:]))
        elif identifier == TXN_ID["STOCK_LEVEL"]:
            transactions.append(transaction.StockLevelTransaction(conn, args[1:]))
        elif identifier == TXN_ID["POPULAR_ITEM"]:
            transactions.append(transaction.PopularItemTransaction(conn, args[1:]))
        elif identifier == TXN_ID["TOP_BALANCE"]:
            transactions.append(transaction.TopBalanceTransaction(conn, args[1:]))
        elif identifier == TXN_ID["RELATED_CUSTOMER"]:
            transactions.append(transaction.RelatedCustomerTransaction(conn, args[1:]))
        else:
            print("UNABLE TO PARSE XACT INPUT: ", args)

        line = file.readline()

    return transactions


def main():
    # Usage: python3 client.py <file> <hostNum> <db>
    # Example: python3 client.py 1.txt -hn 2
    # if hostNum not specified, use default host 2 (i.e. xcnc2)
    parser = argparse.ArgumentParser()
    parser.add_argument("file", metavar="F",
                        type=argparse.FileType('r'),
                        help='Transaction file for client')
    parser.add_argument("-hn", '--hostNum',
                        type=int, default=2,
                        help='Host number e.g. 2 for xcnc2. Default is xcnc2.'
                        )
    parser.add_argument("-p", '--port',
                        type=int, default=26260,
                        help='Port e.g. 26260. Default is 26260.'
                        )
    parser.add_argument("-db", '--database',
                        type=str, default="project",
                        help='Database name'
                        )
    args = parser.parse_args()

    host = f'xcnc{args.hostNum}.comp.nus.edu.sg'
    port = args.port
    user = 'root'  # use root so we don't have to grant privileges manually
    database = args.database
    conn = psycopg2.connect(host=host,
                            port=port,
                            user=user,
                            database=database)

    metrics = MetricsManager()
    transactions = setup_transactions(args.file, conn)
    metrics_filename = os.path.splitext(os.path.basename(args.file.name))[0] + ".metrics"
    args.file.close()

    total_execution_start = datetime.now()
    for txn in transactions:
        transaction_start = datetime.now()
        retry_count = 0
        last_error = ""
        while True:
            try:
                txn.run()
                # Flag issue if transactions had retried more than 15 times (sleep time > 5 seconds)
                if retry_count > 10:
                    sys.stderr.write("Transaction of type " + txn.__class__.__name__ + " retried: " + str(retry_count)
                                     + " times before completion with error: " + last_error + " \n")
                break
            except SerializationFailure as e:
                sleep_ms = (2 ** (retry_count % 16)) * 0.1 * (random.random() + 0.5)
                time.sleep(sleep_ms / 1000)
                last_error = str(e)
                retry_count += 1
                continue
        transaction_end = datetime.now()
        metrics.add(transaction_end - transaction_start)

        txn.print_outputs()
    total_execution_end = datetime.now()
    metrics.add_total_time(total_execution_end - total_execution_start)

    metrics.output_metrics()
    metrics.write_metrics(metrics_filename)
    conn.close()


if __name__ == '__main__':
    main()
