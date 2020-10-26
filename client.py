import math
import sys
from datetime import timedelta
import transaction
import argparse
import psycopg2
from psycopg2.errors import SerializationFailure

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

    # Takes in a timedelta for a transaction
    def add(self, delta):
        self.transaction_timings.append(delta / timedelta(milliseconds=1))

    def output_metrics(self):
        total_time = sum(self.transaction_timings)
        total_transactions = len(self.transaction_timings)
        sorted_transaction_timings = sorted(self.transaction_timings)
        sys.stderr.write("Number of executed transactions: {}\n".format(total_transactions))
        sys.stderr.write("Total transaction execution time (in seconds): {}\n".format(total_time / 1000))
        sys.stderr.write(
            "Transaction throughput (transactions / s): {}\n".format(total_transactions / (total_time / 1000)))
        sys.stderr.write("Average transaction latency (in milliseconds): {}\n".format(total_time / total_transactions))
        sys.stderr.write(
            "Median transaction latency (in milliseconds): {}\n".format(get_percentile(sorted_transaction_timings, 50)))
        sys.stderr.write("95th percentile transaction latency (in milliseconds): {}\n".format(
            get_percentile(sorted_transaction_timings, 95)))
        sys.stderr.write("99th percentile transaction latency (in milliseconds): {}\n".format(
            get_percentile(sorted_transaction_timings, 99)))


def setup_transactions(file, metrics, conn):
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
            transactions.append(transaction.NewOrderTransaction(metrics, conn, inputs))
        elif identifier == TXN_ID["PAYMENT"]:
            transactions.append(transaction.PaymentTransaction(metrics, conn, args[1:]))
        elif identifier == TXN_ID["DELIVERY"]:
            transactions.append(transaction.DeliveryTransaction(metrics, conn, args[1:]))
        elif identifier == TXN_ID["ORDER_STATUS"]:
            transactions.append(transaction.OrderStatusTransaction(metrics, conn, args[1:]))
        elif identifier == TXN_ID["STOCK_LEVEL"]:
            transactions.append(transaction.StockLevelTransaction(metrics, conn, args[1:]))
        elif identifier == TXN_ID["POPULAR_ITEM"]:
            transactions.append(transaction.PopularItemTransaction(metrics, conn, args[1:]))
        elif identifier == TXN_ID["TOP_BALANCE"]:
            transactions.append(transaction.TopBalanceTransaction(metrics, conn, args[1:]))
        elif identifier == TXN_ID["RELATED_CUSTOMER"]:
            transactions.append(transaction.RelatedCustomerTransaction(metrics, conn, args[1:]))
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

    metrics = MetricsManager()
    transactions = setup_transactions(args.file, metrics, conn)
    args.file.close()

    for txn in transactions:
        while True:
            try:
                txn.run()
                break
            except SerializationFailure as e:
                continue

        txn.print_outputs()

    metrics.output_metrics()


if __name__ == '__main__':
    main()
