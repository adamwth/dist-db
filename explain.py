import argparse
import psycopg2
from datetime import datetime

def explain_related_customer(conn, file):
    with conn.cursor() as cur:
        related_customer_query = file.read()

        cur.execute("explain analyze " + str(related_customer_query), {
            "input_warehouse_id": 2,
            "input_customer_id": 6,
            "input_district_id": 1731
        })
        
        print(cur.fetchone())
        

def explain_top_balance(conn, file):
    with conn.cursor() as cur:
        related_customer_query = file.read()

        cur.execute(f'{related_customer_query}', {
            "current_timestamp": datetime.utcnow()
        })
        
        print(cur.fetchone())
        

def explain_popular_item(conn, file):
    with conn.cursor() as cur:
        popular_item_query = file.read()

        cur.execute("explain analyze " + str(popular_item_query), {
            "input_warehouse_id": 1,
            "input_district_id": 7,
            "input_num_last_orders": 47,
            "current_timestamp": datetime.utcnow()            
        })
        
        print(cur.fetchone())

def main():
    # Usage: python3 client.py <file> <hostNum> <port> <db>
    # Example: python3 client.py 1.txt -hn 2 -p 26260
    # if hostNum not specified, use default host 2 (i.e. xcnc2)
    parser = argparse.ArgumentParser()
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
    
    # with open('related-customer.sql', 'r') as f:
    #     explain_related_customer(conn, f)
    with open('top-balance.sql', 'r') as f:
        explain_top_balance(conn, f)
    # with open('popular-item.sql', 'r') as f:
    #     explain_popular_item(conn, f)

if __name__ == '__main__':
    main()
