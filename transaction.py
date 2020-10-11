import sys
from datetime import datetime
from abc import ABC, abstractmethod


class Transaction(ABC):
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

    def end(self):
        self.end_time = datetime.now()

        # Log metrics to manager
        if self.metrics_manager is not None:
            self.metrics_manager.add(self.end_time - self.start_time)

    # To be implemented by subclasses
    @abstractmethod
    def run(self):
        return NotImplementedError


class NewOrderTransaction(Transaction):
    def __init__(self, metrics_manager, conn, inputs):
        super().__init__(metrics_manager)
        self.conn = conn
        # TODO: handle input parsing here

    def run(self):
        with self.conn:
            with self.conn.cursor() as curs:
                self.start()
                # TODO: handle txn here
                self.end()


class PaymentTransaction(Transaction):
    def __init__(self, metrics_manager, conn, inputs):
        super().__init__(metrics_manager)
        self.conn = conn
        # TODO: handle input parsing here

    def run(self):
        with self.conn:
            with self.conn.cursor() as curs:
                self.start()
                # TODO: handle txn here
                self.end()


class DeliveryTransaction(Transaction):
    def __init__(self, metrics_manager, conn, inputs):
        super().__init__(metrics_manager)
        self.conn = conn
        # TODO: handle input parsing here

    def run(self):
        with self.conn:
            with self.conn.cursor() as curs:
                self.start()
                # TODO: handle txn here
                self.end()


class OrderStatusTransaction(Transaction):
    def __init__(self, metrics_manager, conn, inputs):
        super().__init__(metrics_manager)
        self.conn = conn
        # TODO: handle input parsing here

    def run(self):
        with self.conn:
            with self.conn.cursor() as curs:
                self.start()
                # TODO: handle txn here
                self.end()


class StockLevelTransaction(Transaction):
    def __init__(self, metrics_manager, conn, inputs):
        super().__init__(metrics_manager)
        self.conn = conn
        # TODO: handle input parsing here

    def run(self):
        with self.conn:
            with self.conn.cursor() as curs:
                self.start()
                # TODO: handle txn here
                self.end()


class PopularItemTransaction(Transaction):
    def __init__(self, metrics_manager, conn, inputs):
        super().__init__(metrics_manager)
        self.conn = conn
        # TODO: handle input parsing here

    def run(self):
        with self.conn:
            with self.conn.cursor() as curs:
                self.start()
                # TODO: handle txn here
                self.end()


class TopBalanceTransaction(Transaction):
    def __init__(self, metrics_manager, conn, inputs):
        super().__init__(metrics_manager)
        self.conn = conn
        # TODO: handle input parsing here

    def run(self):
        with self.conn:
            with self.conn.cursor() as curs:
                self.start()
                # TODO: handle txn here
                self.end()


class RelatedCustomerTransaction(Transaction):
    def __init__(self, metrics_manager, conn, inputs):
        super().__init__(metrics_manager)
        self.conn = conn
        # TODO: handle input parsing here

    def run(self):
        with self.conn:
            with self.conn.cursor() as curs:
                self.start()
                # TODO: handle txn here
                self.end()
