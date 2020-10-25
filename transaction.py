from abc import ABC, abstractmethod
from datetime import datetime
from decimal import Decimal
import sys


# Returns "(%s, %s, ...), (%s, %s, ...)" meant to be used for VALUES
# args determines number of args in each row
# rows determines number of rows for VALUES


def create_values_placeholder(args, rows):
    single_row = "(" + ("%s," * args)[:-1] + "),"
    return (single_row * rows)[:-1]


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
        self.customer_id = int(inputs[0])
        self.warehouse_id = int(inputs[1])
        self.district_id = int(inputs[2])

        num_items = int(inputs[3])
        self.items = {}
        for i in range(num_items):
            item = inputs[4 + i]
            new_item = {
                "item_no": int(item[0]),
                "supplying_warehouse_no": int(item[1]),
                "quantity": int(item[2]),
                "ol_number": i
            }
            self.items[new_item["item_no"]] = new_item

    def run(self):
        with self.conn:
            with self.conn.cursor() as curs:
                self.start()
                # Get district information, next order id
                curs.execute("SELECT d_next_o_id, d_tax FROM district WHERE d_w_id=%s AND d_id=%s;",
                             (self.warehouse_id, self.district_id))
                order_id, district_tax = curs.fetchone()

                # Update district information
                curs.execute("UPDATE district SET d_next_o_id=%s WHERE d_w_id=%s AND d_id=%s;",
                             (order_id + 1, self.warehouse_id, self.district_id))

                # Create new order entry
                all_local = 1 if all(
                    [x["supplying_warehouse_no"] == self.warehouse_id for x in self.items.values()]) else 0
                entry_date = datetime.now()
                curs.execute("INSERT INTO \"order\" VALUES (%s, %s, %s, %s, %s, %s, %s, %s);", (
                    self.warehouse_id, self.district_id, order_id, self.customer_id, None, len(
                        self.items), all_local,
                    entry_date))

                # Retrieve stock information for items
                total_amount = 0
                values_placeholder = create_values_placeholder(
                    2, len(self.items))
                stock_keys = []
                for item in self.items.values():
                    stock_keys.append(self.warehouse_id)
                    stock_keys.append(item["item_no"])
                curs.execute(
                    "SELECT s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, %s FROM stock WHERE (s_w_id, s_i_id) IN (VALUES " +
                    values_placeholder + ");",
                    ("s_dist_" + str(self.district_id).zfill(2), *stock_keys))
                stocks = curs.fetchall()

                updated_stocks = []
                for stock in stocks:
                    wh_id, i_id, qty, ytd, order_cnt, remote_cnt, dist_info = stock
                    order_qty = self.items[i_id]["quantity"]
                    supply_warehouse_id = self.items[i_id]["supplying_warehouse_no"]

                    new_qty = qty - order_qty
                    if new_qty < 10:
                        new_qty += 100
                    new_remote_cnt = remote_cnt
                    if supply_warehouse_id != self.warehouse_id:
                        new_remote_cnt += 1

                    updated_stocks.append(
                        (wh_id, i_id, new_qty, ytd + order_qty, order_cnt + 1, new_remote_cnt, dist_info))
                    # Keep relevant info for item
                    self.items[i_id]["stocks"] = {
                        "quantity": new_qty,
                        "dist_info": dist_info,
                    }

                # Update stock information for items (use upsert for single update)
                values_placeholder = create_values_placeholder(
                    6, len(updated_stocks))
                updated_stocks_vals = []
                for stock in updated_stocks:
                    updated_stocks_vals.extend(stock[:-1])
                curs.execute(
                    "UPSERT INTO stock (s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt) VALUES " +
                    values_placeholder + ";",
                    updated_stocks_vals)

                # Retrieve item information, track item costs for order
                values_placeholder = create_values_placeholder(
                    len(self.items), 1)
                curs.execute("SELECT i_id, i_name, i_price FROM item WHERE i_id IN " + values_placeholder + ";",
                             list(self.items.keys()))
                items = curs.fetchall()
                for item in items:
                    i_id, name, price = item
                    self.items[i_id]["cost"] = price * \
                        self.items[i_id]["quantity"]
                    self.items[i_id]["name"] = name
                    total_amount += self.items[i_id]["cost"]

                # Add new order lines
                values_placeholder = create_values_placeholder(
                    10, len(self.items))
                new_order_lines = []
                for item in self.items.values():
                    new_order_lines.extend([self.warehouse_id, self.district_id, order_id, item["ol_number"],
                                            item["item_no"], None, item["cost"], item["supplying_warehouse_no"],
                                            item["quantity"], item["stocks"]["dist_info"]])
                curs.execute("INSERT INTO orderline VALUES " +
                             values_placeholder + ";", new_order_lines)

                # Retrieve user information
                curs.execute(
                    "SELECT c_last, c_credit, c_discount FROM customer WHERE c_w_id=%s AND c_d_id=%s AND c_id=%s;",
                    (self.warehouse_id, self.district_id, self.customer_id))
                last_name, credit, discount = curs.fetchone()

                # Retrieve warehouse tax
                curs.execute(
                    "SELECT w_tax FROM warehouse WHERE w_id=%s;", (self.warehouse_id,))
                warehouse_tax = curs.fetchone()[0]

                # Calculate total amount
                total_amount = total_amount * \
                    (1 + district_tax + warehouse_tax) * (1 - discount)

                # Add to output dict
                self.outputs["Customer identifier"] = "({}, {}, {})".format(self.warehouse_id, self.district_id,
                                                                            self.customer_id)
                self.outputs["Customer last name"] = last_name
                self.outputs["Customer credit"] = credit
                self.outputs["Customer discount"] = discount
                self.outputs["Warehouse tax rate"] = warehouse_tax
                self.outputs["District tax rate"] = district_tax
                self.outputs["Order number"] = order_id
                self.outputs["Entry date"] = entry_date
                self.outputs["Num items"] = len(self.items)
                self.outputs["Total amount"] = total_amount
                for item in self.items.values():
                    item_name = "Item {}".format(item["item_no"])
                    self.outputs[item_name] = "{} x {} from warehouse {} costing {} with remaining stock {}".format(
                        item["name"], item["quantity"], item["supplying_warehouse_no"], item["cost"],
                        item["stocks"]["quantity"])
        self.end()


class PaymentTransaction(Transaction):
    def __init__(self, metrics_manager, conn, inputs):
        super().__init__(metrics_manager)
        self.conn = conn
        self.warehouse_id = int(inputs[0])
        self.district_id = int(inputs[1])
        self.customer_id = int(inputs[2])
        self.payment = float(inputs[3])
        self.payment_decimal = Decimal(inputs[3])

    def run(self):
        with self.conn:
            with self.conn.cursor() as curs:
                self.start()
                # Fetch and update warehouse details
                curs.execute(
                    "SELECT w_ytd, w_street_1, w_street_2, w_city, w_state, w_zip FROM warehouse WHERE w_id=%s;",
                    (self.warehouse_id,))
                w_ytd, w_street1, w_street2, w_city, w_state, w_zip = curs.fetchone()
                curs.execute("UPDATE warehouse SET w_ytd = %s WHERE w_id=%s;",
                             (w_ytd + self.payment_decimal, self.warehouse_id))

                # Fetch and update district details
                curs.execute(
                    "SELECT d_ytd, d_street_1, d_street_2, d_city, d_state, d_zip FROM district WHERE d_w_id=%s AND d_id=%s;",
                    (self.warehouse_id, self.district_id))
                d_ytd, d_street1, d_street2, d_city, d_state, d_zip = curs.fetchone()
                curs.execute("UPDATE district SET d_ytd = %s WHERE d_w_id=%s AND d_id=%s;",
                             (d_ytd + self.payment_decimal, self.warehouse_id, self.district_id))

                # Fetch and update customer details
                curs.execute(
                    "SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt FROM customer WHERE c_w_id=%s AND c_d_id=%s AND c_id=%s;",
                    (self.warehouse_id, self.district_id, self.customer_id))
                first_name, middle_name, last_name, c_street1, c_street2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd, c_payment_count = curs.fetchone()
                new_balance = c_balance - self.payment_decimal
                new_ytd_payment = c_ytd + self.payment
                new_payment_cnt = c_payment_count + 1
                curs.execute(
                    "UPDATE customer SET c_balance=%s, c_ytd_payment=%s, c_payment_cnt=%s WHERE c_w_id=%s AND c_d_id=%s AND c_id=%s;",
                    (new_balance, new_ytd_payment, new_payment_cnt, self.warehouse_id, self.district_id,
                     self.customer_id))

                # Add to output dict
                self.outputs["Customer identifier"] = "({}, {}, {})".format(self.warehouse_id, self.district_id,
                                                                            self.customer_id)
                self.outputs["Customer name"] = "{} {} {}".format(
                    first_name, middle_name, last_name)
                self.outputs["Customer address"] = "{} {} {} {} {}".format(
                    c_street1, c_street2, c_city, c_state, c_zip)
                self.outputs["Customer phone"] = c_phone
                self.outputs["Customer creation date"] = c_since
                self.outputs["Customer credit"] = c_credit
                self.outputs["Customer credit limit"] = c_credit_lim
                self.outputs["Customer discount"] = c_discount
                self.outputs["Customer balance"] = new_balance
                self.outputs["Warehouse address"] = "{} {} {} {} {}".format(w_street1, w_street2, w_city, w_state,
                                                                            w_zip)
                self.outputs["District address"] = "{} {} {} {} {}".format(
                    d_street1, d_street2, d_city, d_state, d_zip)
                self.outputs["Payment"] = self.payment
        self.end()


class DeliveryTransaction(Transaction):
    def __init__(self, metrics_manager, conn, inputs):
        super().__init__(metrics_manager)
        self.conn = conn
        self.warehouse_id = int(inputs[0])
        self.carrier_id = int(inputs[1])

    def run(self):
        with self.conn:
            with self.conn.cursor() as curs:
                self.start()

                # Retrieve orders to be processed by carrier
                curs.execute(
                    "SELECT o_w_id, o_d_id, o_id , o_c_id FROM \"order\" WHERE (o_w_id, o_d_id, o_id) IN (SELECT o_w_id, o_d_id, MIN(o_id) FROM \"order\" GROUP BY o_w_id, o_d_id, o_carrier_id HAVING o_carrier_id IS NULL AND o_w_id=%s);",
                    (self.warehouse_id,))
                orders = curs.fetchall()

                # Form list of customers to track
                customer_amounts = {}
                order_customer_mapping = {}
                for order in orders:
                    customer_id = (*order[:2], order[3])
                    order_customer_mapping[tuple(order[:3])] = customer_id
                    customer_amounts[customer_id] = 0

                # Update orders
                values_placeholder = create_values_placeholder(3, len(orders))
                order_keys = []
                for order in orders:
                    order_keys.extend(order[:3])
                curs.execute(
                    "UPDATE \"order\" SET o_carrier_id=%s WHERE (o_w_id, o_d_id, o_id) IN (VALUES " +
                    values_placeholder + ");",
                    (self.carrier_id, *order_keys))

                # Update order lines and fetch order amounts
                delivery_date = datetime.now()
                curs.execute(
                    "UPDATE orderline SET ol_delivery_d=%s WHERE (ol_w_id, ol_d_id, ol_o_id) IN (VALUES " +
                    values_placeholder + ") RETURNING ol_w_id, ol_d_id, ol_o_id, ol_number, ol_amount;",
                    (delivery_date, *order_keys))

                order_lines = curs.fetchall()
                for order_line in order_lines:
                    customer_id = order_customer_mapping[tuple(order_line[:3])]
                    customer_amounts[customer_id] += order_line[-1]

                # Fetch and update customers
                values_placeholder = create_values_placeholder(
                    3, len(customer_amounts))
                customer_keys = []
                for customer_id in customer_amounts.keys():
                    customer_keys.extend(customer_id)
                curs.execute(
                    "SELECT c_w_id, c_d_id, c_id, c_balance, c_delivery_cnt FROM customer WHERE (c_w_id, c_d_id, c_id) IN (VALUES " +
                    values_placeholder + ");",
                    customer_keys)

                customers = curs.fetchall()
                values_placeholder = create_values_placeholder(
                    5, len(customers))
                updated_customers_vals = []
                for customer in customers:
                    customer_id = tuple(customer[:3])
                    updated_customers_vals.extend(
                        (*customer_id, customer[3] + customer_amounts[customer_id], customer[4] + 1))

                curs.execute(
                    "UPSERT INTO customer (c_w_id, c_d_id, c_id, c_balance, c_delivery_cnt) VALUES " +
                    values_placeholder + ";",
                    updated_customers_vals)
        self.end()


class OrderStatusTransaction(Transaction):
    def __init__(self, metrics_manager, conn, inputs):
        super().__init__(metrics_manager)
        self.conn = conn
        self.warehouse_id = int(inputs[0])
        self.district_id = int(inputs[1])
        self.customer_id = int(inputs[2])

    def run(self):
        with self.conn:
            with self.conn.cursor() as curs:
                self.start()

                # Fetch customer information
                curs.execute(
                    "SELECT c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id=%s AND c_d_id=%s AND c_id=%s;",
                    (self.warehouse_id, self.district_id, self.customer_id))
                first_name, middle_name, last_name, balance = curs.fetchone()

                # Fetch items in last order
                curs.execute(
                    "SELECT o_id, o_entry_d, o_carrier_id, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM order JOIN orderline WHERE ol_w_id = o_w_id AND ol_d_id = o_d_id AND ol_o_id = o_id AND o_c_id = c_id ORDER BY o_entry_d DESC LIMIT 1;")
                items = curs.fetchall()

                # Add to output
                self.outputs["Customer name"] = "%s %s %s".format(
                    first_name, middle_name, last_name)
                self.outputs["Customer balance"] = balance

                order_items = []
                for item in items:
                    order_item = {"number": item[3], "supplying_warehouse": item[4], "quantity": item[5],
                                  "price": item[6], "delivery_date": item[7]}
                    order_items.append(order_item)
                    item_name = "Item %s".format(item[3])
                    self.outputs[item_name] = "%d from warehouse %d costing %d delivered at %d".format(item[5], item[4],
                                                                                                       item[6], item[7])
                if len(items) > 0:
                    order_name = "Order %s".format(item[0])
                    self.outputs[order_name] = "ordered at %s with carrier %d".format(
                        item[1], item[2])
        self.end()


class StockLevelTransaction(Transaction):
    def __init__(self, metrics_manager, conn, inputs):
        super().__init__(metrics_manager)
        self.conn = conn
        self.warehouse_id = int(inputs[0])
        self.district_id = int(inputs[1])
        self.stock_threshold = int(inputs[2])
        self.num_last_orders = int(inputs[3])

    def run(self):
        with self.conn:
            with self.conn.cursor() as curs:
                self.start()
                # Read next order number for district
                curs.execute("SELECT d_next_o_id FROM district WHERE d_w_id=%s AND d_id=%s;",
                             (self.warehouse_id, self.district_id))
                next_order_id = curs.fetchone()

                # Count number of items with stock below threshold
                curs.execute(
                    "SELECT COUNT(DISTINCT S_I_ID) FROM orderline JOIN stock WHERE ol_i_id = s_i_id AND ol_w_id = s_w_id AND ol_w_id=%s AND ol_d_id=%s AND s_quantity < %s AND ol_o_id >= %s AND ol_o_id < %s;",
                    (self.warehouse_id, self.district_id, self.stock_threshold, next_order_id - self.num_last_orders,
                     next_order_id))
                num_items = curs.fetchone()

                # Add to output
                self.outputs["Number of items below stock threshold"] = num_items
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
        self.warehouse_id = int(inputs[0])
        self.district_id = int(inputs[1])
        self.customer_id = int(inputs[2])

    def run(self):
        with self.conn:
            with self.conn.cursor() as curs:
                with open('related-customer.sql', 'r') as f:
                    self.start()
                    curs.execute(f.read(), {
                        "input_warehouse_id": self.warehouse_id,
                        "input_customer_id": self.customer_id
                    })
                    relatedCustomers = curs.fetchall()
                    self.outputs['related customers'] = {
                        "input customer": {
                            warehouse_id: self.warehouse_id,
                            district_id: self.district_id,
                            customer_id: self.customer_id
                        },
                        "related customers": relatedCustomers
                    }
        self.end()
