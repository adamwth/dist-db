import sys
from datetime import datetime
from abc import ABC, abstractmethod


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
        self.warehouse_id = int(inputs[0])
        self.district_id = int(inputs[1])
        self.customer_id = int(inputs[2])

        num_items = int(inputs[3])
        self.items = {}
        for i in range(num_items):
            start_index = i * 3 + 2
            new_item = {
                "item_no": int(inputs[start_index]),
                "supplying_warehouse_no": inputs[start_index + 1],
                "quantity": inputs[start_index + 2],
                "ol_number": i
            }
            self.items[new_item["item_no"]] = new_item

    def run(self):
        with self.conn:
            with self.conn.cursor() as curs:
                self.start()
                # Get district information, next order id
                curs.execute("SELECT D_NEXT_O_ID, D_TAX FROM District WHERE D_W_ID=%s AND D_ID=%s;",
                             (self.warehouse_id, self.district_id))
                order_id, district_tax = curs.fetchone()

                # Update district information
                curs.execute("UPDATE District SET D_NEXT_O_ID=%s WHERE D_W_ID=%s AND D_ID=%s;",
                             (order_id + 1, self.warehouse_id, self.district_id))

                # Create new order entry
                all_local = 1 if all(
                    [x["supplying_warehouse_no"] == self.warehouse_id for x in self.items.values()]) else 0
                entry_date = datetime.now()
                curs.execute("INSERT INTO Order VALUES (%s, %s, %s, %s, %s, %s, %s, %s);", (
                    self.warehouse_id, self.district_id, order_id, self.customer_id, None, len(self.items), all_local,
                    entry_date))

                # Retrieve stock information for items
                total_amount = 0
                values_placeholder = create_values_placeholder(2, len(self.items))
                stock_keys = []
                for item in self.items.values():
                    stock_keys.append(self.warehouse_id)
                    stock_keys.append(item["item_no"])
                curs.execute(
                    "SELECT S_W_ID, S_I_ID, S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_%s FROM Stock WHERE (S_W_ID, S_I_ID) IN (VALUES " + values_placeholder + ");",
                    (str(self.district_id).zfill(2), *stock_keys))
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
                values_placeholder = create_values_placeholder(6, len(updated_stocks))
                updated_stocks_vals = []
                for stock in updated_stocks:
                    updated_stocks_vals.extend(stock[:-1])
                curs.execute(
                    "UPSERT INTO Stock (S_W_ID, S_I_ID, S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT) VALUES " + values_placeholder + ";",
                    updated_stocks_vals)

                # Retrieve item information, track item costs for order
                values_placeholder = create_values_placeholder(len(self.items), 1)
                curs.execute("SELECT I_ID, I_NAME, I_PRICE FROM Item WHERE I_ID IN " + values_placeholder + ";",
                             list(self.items.keys()))
                items = curs.fetchall()
                for item in items:
                    i_id, name, price = item
                    self.items[i_id]["cost"] = price * self.items[i_id]["quantity"]
                    self.items[i_id]["name"] = name
                    total_amount += self.items[i_id]["cost"]

                # Add new order lines
                values_placeholder = create_values_placeholder(10, len(self.items))
                new_order_lines = []
                for item in self.items.values():
                    new_order_lines.extend([self.warehouse_id, self.district_id, order_id, item["ol_number"],
                                            item["item_no"], None, item["cost"], item["supplying_warehouse_no"],
                                            item["quantity"], item["stocks"]["dist_info"]])
                curs.execute("INSERT INTO OrderLine VALUES " + values_placeholder + ";")

                # Retrieve user information
                curs.execute(
                    "SELECT C_LAST, C_CREDIT, C_DISCOUNT FROM Customer WHERE C_W_ID=%s AND C_D_ID=%s AND C_ID=%s;",
                    (self.warehouse_id, self.district_id, self.customer_id))
                last_name, credit, discount = curs.fetchone()

                # Retrieve warehouse tax
                curs.execute("SELECT W_TAX FROM Warehouse WHERE W_ID=%s;", (self.warehouse_id,))
                warehouse_tax = curs.fetchone()

                # Calculate total amount
                total_amount = total_amount * (1 + district_tax + warehouse_tax) * (1 - discount)

                # Add to output dict
                self.outputs["Customer identifier"] = "(%d, %d, %d)".format(self.warehouse_id, self.district_id,
                                                                            self.customer_id)
                self.outputs["Customer last name"] = last_name
                self.outputs["Customer credit"] = credit
                self.outputs["Customer discount"] = discount
                self.outputs["Warehouse tax rate"] = warehouse_tax
                self.outputs["District tax rate"] = district_tax
                self.outputs["Order number"] = order_id
                self.outputs["Entry date"] = entry_date
                self.outputs["Num items"] = len(self.items)
                for item in self.items.values():
                    item_name = "Item %s".format(item["item_no"])
                    self.outputs[item_name] = "%s x %d from warehouse %d costing %d with remaining stock %d".format(
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
        self.payment = int(inputs[3])

    def run(self):
        with self.conn:
            with self.conn.cursor() as curs:
                self.start()
                # Fetch and update warehouse details
                curs.execute(
                    "SELECT W_YTD, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM Warehouse WHERE W_ID=%s;",
                    (self.warehouse_id,))
                w_ytd, w_street1, w_street2, w_city, w_state, w_zip = curs.fetchone()
                curs.execute("UPDATE Warehouse SET W_YTD = %s WHERE W_ID=%s;",
                             (w_ytd + self.payment, self.warehouse_id))

                # Fetch and update district details
                curs.execute(
                    "SELECT D_YTD, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM District WHERE D_W_ID=%s AND D_ID=%s;",
                    (self.warehouse_id, self.district_id))
                d_ytd, d_street1, d_street2, d_city, d_state, d_zip = curs.fetchone()
                curs.execute("UPDATE District SET D_YTD = %s WHERE D_W_ID=%s AND D_ID=%s;",
                             (d_ytd + self.payment, self.warehouse_id, self.district_id))

                # Fetch and update customer details
                curs.execute(
                    "SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT FROM Customer WHERE C_W_ID=%s AND C_D_ID=%s AND C_ID=%s;",
                    (self.warehouse_id, self.district_id, self.customer_id))
                first_name, middle_name, last_name, c_street1, c_street2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd, c_payment_count = curs.fetchone()
                new_balance = c_balance - self.payment
                new_ytd_payment = c_ytd + self.payment
                new_payment_cnt = c_payment_count + 1
                curs.execute(
                    "UPDATE Customer SET C_BALANCE=%s, C_YTD_PAYMENT=%s, C_PAYMENT_CNT=%s WHERE C_W_ID=%s AND C_D_ID=%s AND C_ID=%s;",
                    (new_balance, new_ytd_payment, new_payment_cnt, self.warehouse_id, self.district_id,
                     self.customer_id))

                # Add to output dict
                self.outputs["Customer identifier"] = "(%d, %d, %d)".format(self.warehouse_id, self.district_id,
                                                                            self.customer_id)
                self.outputs["Customer name"] = "%s %s %s".format(first_name, middle_name, last_name)
                self.outputs["Customer address"] = "%s %s %s %s %s".format(c_street1, c_street2, c_city, c_state, c_zip)
                self.outputs["Customer phone"] = c_phone
                self.outputs["Customer creation date"] = c_since
                self.outputs["Customer credit"] = c_credit
                self.outputs["Customer credit limit"] = c_credit_lim
                self.outputs["Customer discount"] = c_discount
                self.outputs["Customer balance"] = new_balance
                self.outputs["Warehouse address"] = "%s %s %s %s %s".format(w_street1, w_street2, w_city, w_state,
                                                                            w_zip)
                self.outputs["District address"] = "%s %s %s %s %s".format(d_street1, d_street2, d_city, d_state, d_zip)
                self.outputs["Payment"] = self.payment
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
