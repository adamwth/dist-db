import argparse
import psycopg2


def main():
    # Usage: python3 output_state.py <output file name> <hostNum> <db>
    # Example: python3 client.py exp1-out -hn 2
    # if hostNum not specified, use default host 2 (i.e. xcnc2)
    parser = argparse.ArgumentParser()
    parser.add_argument("file", metavar="F",
                        type=argparse.FileType('w'),
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

    # host = f'xcnc{args.hostNum}.comp.nus.edu.sg'
    # port = '26259'
    host = f'localhost'
    port = '26257'
    user = 'admin'  # use admin so we don't have to grant privileges manually
    database = args.database
    conn = psycopg2.connect(host=host,
                            port=port,
                            user=user,
                            database=database)

    state = []

    with conn.cursor() as curs:
        curs.execute("SELECT SUM(w_ytd) FROM warehouse;")
        state.extend(curs.fetchone())
        curs.execute("SELECT SUM(d_ytd), SUM(d_next_o_id) FROM district;")
        state.extend(curs.fetchone())
        curs.execute(
            "SELECT SUM(c_balance), SUM(c_ytd_payment), SUM(c_payment_cnt), SUM(c_delivery_cnt) FROM customer;")
        state.extend(curs.fetchone())
        curs.execute("SELECT MAX(o_id), SUM(o_ol_cnt) FROM \"order\";")
        state.extend(curs.fetchone())
        curs.execute("SELECT SUM(ol_amount), SUM(ol_quantity) FROM orderline;")
        state.extend(curs.fetchone())
        curs.execute("SELECT SUM(s_quantity), SUM(s_ytd), SUM(s_order_cnt), SUM(s_remote_cnt) FROM stock;")
        state.extend(curs.fetchone())

        assert len(state) == 15
        print(state)
        state_string = [str(x) for x in state]
        args.file.write(",".join(state_string))
        args.file.close()


if __name__ == '__main__':
    main()
