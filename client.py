import math
import sys
from datetime import datetime
from datetime import timedelta
import time


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


class Transaction:
    def __init__(self, metrics_manager):
        self.inputs = {}
        self.outputs = {}
        self.metrics_manager = metrics_manager
        self.start_time = None
        self.end_time = None

    def print_outputs(self):
        for k, v in self.outputs.items():
            sys.stdout.write("{}: {}\n".format(k, v))

    def start(self):
        self.start_time = datetime.now()
        self.outputs["hello"] = "flower"

    def end(self):
        self.end_time = datetime.now()

        # Log metrics to manager
        if self.metrics_manager is not None:
            self.metrics_manager.add(self.end_time - self.start_time)


def read_xact_file(path):
    pass


def handle_transaction(transaction):
    pass


def test_metrics_manager():
    metrics = MetricsManager()
    for i in range(1, 1000):
        txn = Transaction(metrics)
        txn.start()
        time.sleep(i / 1000000)
        txn.end()
        txn.print_outputs()
    metrics.output_metrics()


if __name__ == '__main__':
    test_metrics_manager()
