from abc import ABC, abstractmethod
from datetime import datetime
from decimal import Decimal
import sys

# Returns "(%s, %s, ...), (%s, %s, ...)" meant to be used for VALUES
# args determines number of args in each row
# rows determines number of rows for VALUES
from psycopg2.extras import execute_batch


def create_values_placeholder(args, rows):
    single_row = "(" + ("%s," * args)[:-1] + "),"
    return (single_row * rows)[:-1]


class Transaction(ABC):
    def __init__(self):
        self.inputs = {}
        self.outputs = {}

    def print_outputs(self):
        for k, v in self.outputs.items():
            sys.stdout.write("{}: {}\n".format(k, v))

    # To be implemented by subclasses
    @abstractmethod
    def run(self):
        return NotImplementedError


class NewOrderTransaction(Transaction):
    def __init__(self, conn, inputs):
        super().__init__()
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
                "ol_number": i,
            }
            self.items[new_item["item_no"]] = new_item

    def run(self):
        with self.conn:
            with self.conn.cursor() as curs:
                # Fetch next order id and district information, updating next order id
                curs.execute(
                    """
                    UPDATE district 
                    SET d_next_o_id = d_next_o_id + 1
                    WHERE d_w_id=%s AND d_id=%s
                    RETURNING d_next_o_id, d_tax;
                    """,
                    (self.warehouse_id, self.district_id))
                next_order_id, district_tax = curs.fetchone()
                order_id = next_order_id - 1

                # Create new order entry
                all_local = 1 if all(
                    [x["supplying_warehouse_no"] == self.warehouse_id for x in self.items.values()]) else 0
                entry_date = datetime.utcnow()
                curs.execute("INSERT INTO \"order\" VALUES (%s, %s, %s, %s, %s, %s, %s, %s);", (
                    self.warehouse_id, self.district_id, order_id, self.customer_id, None, len(
                        self.items), all_local,
                    entry_date))

                # Retrieve stock information for items
                total_amount = 0
                district_field = "s_dist_" + str(self.district_id).zfill(2)
                values_placeholder = create_values_placeholder(
                    2, len(self.items))
                stock_keys = []
                for item in self.items.values():
                    stock_keys.append((district_field, self.warehouse_id, item["item_no"]))

                stocks = []
                for stock_key in stock_keys:
                    curs.execute(
                        """
                        SELECT s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, %s 
                        FROM stock 
                        WHERE s_w_id = %s AND s_i_id = %s;
                        """,
                        stock_key)
                    res = curs.fetchone()
                    if res:
                        stocks.append(res)

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
                order_items = []
                for item in self.items.values():
                    order_item = {"number": item["item_no"],
                                  "name": item["name"],
                                  "supplying_warehouse": item["supplying_warehouse_no"],
                                  "quantity": item["quantity"],
                                  "price": item["cost"],
                                  "remaining_stock": item["stocks"]["quantity"]}
                    order_items.append(order_item)
                self.outputs["Items"] = order_items


class PaymentTransaction(Transaction):
    def __init__(self, conn, inputs):
        super().__init__()
        self.conn = conn
        self.warehouse_id = int(inputs[0])
        self.district_id = int(inputs[1])
        self.customer_id = int(inputs[2])
        self.payment = float(inputs[3])

    def run(self):
        with self.conn:
            with self.conn.cursor() as curs:
                # Fetch and update warehouse details
                curs.execute(
                    """
                    UPDATE warehouse SET w_ytd = w_ytd + %s
                    WHERE w_id=%s
                    RETURNING w_street_1, w_street_2, w_city, w_state, w_zip;
                    """,
                    (self.payment, self.warehouse_id))
                w_street1, w_street2, w_city, w_state, w_zip = curs.fetchone()

                # Fetch and update district details
                curs.execute(
                    """
                    UPDATE district
                    SET d_ytd = d_ytd + %s
                    WHERE d_w_id=%s AND d_id=%s
                    RETURNING d_street_1, d_street_2, d_city, d_state, d_zip
                    """,
                    (self.payment, self.warehouse_id, self.district_id))
                d_street1, d_street2, d_city, d_state, d_zip = curs.fetchone()

                # Fetch and update customer details
                curs.execute(
                    """
                    UPDATE customer
                    SET c_balance = c_balance - %s, c_ytd_payment = c_ytd_payment + %s, c_payment_cnt = c_payment_cnt + 1
                    WHERE c_w_id=%s AND c_d_id=%s AND c_id=%s
                    RETURNING c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, 
                    c_since, c_credit, c_credit_lim, c_discount, c_balance;
                    """,
                    (self.payment, self.payment, self.warehouse_id, self.district_id, self.customer_id))
                first_name, middle_name, last_name, c_street1, c_street2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance = curs.fetchone()

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
                self.outputs["Customer balance"] = c_balance
                self.outputs["Warehouse address"] = "{} {} {} {} {}".format(w_street1, w_street2, w_city, w_state,
                                                                            w_zip)
                self.outputs["District address"] = "{} {} {} {} {}".format(
                    d_street1, d_street2, d_city, d_state, d_zip)
                self.outputs["Payment"] = self.payment


class DeliveryTransaction(Transaction):
    def __init__(self, conn, inputs):
        super().__init__()
        self.conn = conn
        self.warehouse_id = int(inputs[0])
        self.carrier_id = int(inputs[1])

    def run(self):
        with self.conn:
            with self.conn.cursor() as curs:

                orders = []
                for i in range(1, 11):
                    curs.execute(
                        """
                        SELECT o_w_id, o_d_id, o_id, o_c_id FROM "order"
                        WHERE o_w_id = %s AND o_d_id = %s
                        AND o_carrier_id IS NULL
                        ORDER BY o_id
                        LIMIT 1
                        """, (self.warehouse_id, i))

                    res = curs.fetchone()
                    if res:
                        orders.append(res)

                order_keys = []
                for order in orders:
                    order_keys.append(order[:3])

                # Only process if there are orders to deliver on
                if len(order_keys) > 0:
                    # Update orders
                    customer_balance = {}
                    order_customer_mapping = {}
                    for order in orders:
                        customer_id = (*order[:2], order[3])
                        order_customer_mapping[tuple(order[:3])] = customer_id
                        customer_balance[customer_id] = 0

                    execute_batch(curs,
                                  """
                                  UPDATE "order" 
                                  SET o_carrier_id=%s WHERE o_w_id = %s AND o_d_id = %s AND o_id = %s;
                                  """,
                                  [[self.carrier_id, *order_key] for order_key in order_keys])

                    # Update order lines and fetch order amounts
                    order_lines = []
                    delivery_date = datetime.utcnow()
                    for order_key in order_keys:
                        curs.execute(
                            """
                            UPDATE orderline 
                            SET ol_delivery_d = %s 
                            WHERE ol_w_id = %s AND ol_d_id = %s AND ol_o_id = %s
                            RETURNING ol_w_id, ol_d_id, ol_o_id, ol_number, ol_amount;
                            """,
                            (delivery_date, *order_key))
                        res = curs.fetchone()
                        if res:
                            order_lines.append(res)

                    for order_line in order_lines:
                        customer_id = order_customer_mapping[tuple(order_line[:3])]
                        customer_balance[customer_id] += order_line[-1]

                    # Update customers
                    updated_customers = []
                    for customer_id, balance_addition in customer_balance.items():
                        updated_customers.append((balance_addition, *customer_id))

                    execute_batch(curs,
                                  """
                                  UPDATE customer
                                  SET c_delivery_cnt = c_delivery_cnt + 1,
                                  c_balance = c_balance + %s
                                  WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s;
                                  """,
                                  updated_customers)


class OrderStatusTransaction(Transaction):
    def __init__(self, conn, inputs):
        super().__init__()
        self.conn = conn
        self.warehouse_id = int(inputs[0])
        self.district_id = int(inputs[1])
        self.customer_id = int(inputs[2])

    def run(self):
        with self.conn:
            with self.conn.cursor() as curs:

                # Fetch customer information
                curs.execute(
                    """
                    SELECT c_first, c_middle, c_last, c_balance, o_id
                    FROM customer 
                    JOIN "order"
                    ON o_w_id = c_w_id AND o_d_id = c_d_id AND o_c_id = c_id
                    WHERE c_w_id=%s AND c_d_id=%s AND c_id=%s
                    ORDER BY o_id DESC
                    LIMIT 1;
                    """,
                    (self.warehouse_id, self.district_id, self.customer_id))
                first_name, middle_name, last_name, balance, last_o_id = curs.fetchone()

                # Fetch items in last order
                curs.execute(
                    """
                    SELECT o_id, o_entry_d, o_carrier_id, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d 
                    FROM "order" 
                    JOIN orderline ON ol_w_id = o_w_id AND ol_d_id = o_d_id AND ol_o_id = o_id  
                    WHERE o_w_id=%s AND o_d_id=%s AND o_id=%s;
                    """,
                    (self.warehouse_id, self.district_id, last_o_id))
                items = curs.fetchall()

                # Add to output
                self.outputs["Customer name"] = "{} {} {}".format(
                    first_name, middle_name, last_name)
                self.outputs["Customer balance"] = balance

                if len(items) > 0:
                    order_name = "Order {}".format(items[0][0])
                    self.outputs[order_name] = "ordered at {} with carrier {}".format(
                        items[0][1], items[0][2])
                order_items = []
                for item in items:
                    order_item = {"number": item[3], "supplying_warehouse": item[4], "quantity": item[5],
                                  "price": item[6], "delivery_date": item[7]}
                    order_items.append(order_item)
                self.outputs["Order items"] = order_items


class StockLevelTransaction(Transaction):
    def __init__(self, conn, inputs):
        super().__init__()
        self.conn = conn
        self.warehouse_id = int(inputs[0])
        self.district_id = int(inputs[1])
        self.stock_threshold = int(inputs[2])
        self.num_last_orders = int(inputs[3])

    def run(self):
        with self.conn:
            with self.conn.cursor() as curs:
                # Read next order number for district
                curs.execute("SELECT d_next_o_id FROM district WHERE d_w_id=%s AND d_id=%s;",
                             (self.warehouse_id, self.district_id))
                next_order_id = curs.fetchone()[0]

                # Count number of items with stock below threshold
                curs.execute(
                    """
                    SELECT COUNT(s_i_id) 
                    FROM orderline 
                    JOIN stock ON ol_i_id = s_i_id AND ol_w_id = s_w_id 
                    WHERE ol_w_id=%s AND ol_d_id=%s AND s_quantity < %s AND ol_o_id >= %s AND ol_o_id < %s;
                    """,
                    (self.warehouse_id, self.district_id, self.stock_threshold, next_order_id - self.num_last_orders,
                     next_order_id))
                num_items = curs.fetchone()[0]

                # Add to output
                self.outputs["Number of items below stock threshold"] = num_items


class PopularItemTransaction(Transaction):
    def __init__(self, conn, inputs):
        super().__init__()
        self.conn = conn
        self.warehouse_id = int(inputs[0])
        self.district_id = int(inputs[1])
        self.num_last_orders = int(inputs[2])

    def run(self):
        with self.conn:
            with self.conn.cursor() as curs:
                with open('popular-item.sql', 'r') as f:
                    popular_items_query = f.read()

                    # Get all orders with popular items
                    curs.execute(popular_items_query, {
                        "input_warehouse_id": self.warehouse_id,
                        "input_district_id": self.district_id,
                        "input_num_last_orders": self.num_last_orders
                    })
                    orders = curs.fetchall()

                    # Dictionaries to store output
                    order_map = {}
                    pop_item_statistics = {}

                    # Store order info and popular items for each order
                    for x in orders:
                        order_id = x[0]
                        order_entry_date = x[1]
                        c_first = x[2]
                        c_middle = x[3]
                        c_last = x[4]
                        item_name = x[5]
                        quantity = x[6]
                        if order_id not in order_map:
                            order_map[order_id] = {
                                "order_id": order_id,
                                "order_entry_date": order_entry_date,
                                "c_first": c_first,
                                "c_middle": c_middle,
                                "c_last": c_last,
                                "pop_items": {}
                            }
                        order_map[order_id]['pop_items'][item_name] = quantity
                        pop_item_statistics[item_name] = 0

                    # Store percentage of orders that contain each popular item
                    total_orders = len(order_map)
                    for key in pop_item_statistics:
                        num_containing_orders = 0
                        for _, value in order_map.items():
                            if key in value['pop_items']:
                                num_containing_orders += 1
                        pop_item_statistics[key] = "{}%".format((num_containing_orders * 100 / total_orders))

                    self.outputs["District identifier"] = "({}, {})".format(self.warehouse_id, self.district_id)
                    self.outputs["Number of last orders examined"] = total_orders
                    self.outputs["Orders with popular items"] = order_map
                    self.outputs['Popular item statistics'] = pop_item_statistics


class TopBalanceTransaction(Transaction):
    def __init__(self, conn, inputs):
        super().__init__()
        self.conn = conn

    def run(self):
        with self.conn:
            with self.conn.cursor() as curs:
                with open('top-balance.sql', 'r') as f:
                    top_balance_query = f.read()
                    curs.execute(top_balance_query, {
                        "current_timestamp": datetime.utcnow()
                    })
                    customers_top_balance = curs.fetchall()

                    # Add to outputs
                    self.outputs["Top 10 customers with highest balance"] = [
                        "({}, {}, {}, {}, {}, {})".format(*x) for x in customers_top_balance]


class RelatedCustomerTransaction(Transaction):
    def __init__(self, conn, inputs):
        super().__init__()
        self.conn = conn
        self.warehouse_id = int(inputs[0])
        self.district_id = int(inputs[1])
        self.customer_id = int(inputs[2])

    def run(self):
        with self.conn:
            with self.conn.cursor() as curs:
                with open('related-customer.sql', 'r') as f:
                    related_customer_query = f.read()

                    curs.execute(related_customer_query, {
                        "input_warehouse_id": self.warehouse_id,
                        "input_customer_id": self.customer_id,
                        "input_district_id": self.district_id,
                        "current_timestamp": datetime.utcnow()
                    })
                    related_customers = curs.fetchall()

                    # Add to outputs
                    self.outputs["Input customer identifier"] = "({}, {}, {})".format(self.warehouse_id,
                                                                                      self.district_id,
                                                                                      self.customer_id)
                    self.outputs['Related customers'] = [
                        "({}, {}, {})".format(*x) for x in related_customers]
