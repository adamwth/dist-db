# dist-db

# Requirements

- Python 3.6 (assumes aliases `python3` and `pip3`)
- CockroachDB v19.2.9 cluster on `xcnc` machines
  - Run in insecure mode, scripts will use root user

# Environment Setup

The only dependency required is psycopg2==2.8.6 (found under requirements.txt)

Can be installed using `pip3 install -r requirements.txt --user`

# Scripts

### setup.py

Sets up the cluster for experiment by loading in the initial data using IMPORT INTO (drops existing tables)

For CockroachDB v19.2.9, it requires that csv data files are present **on every node** under `{node store}/extern/data-files/`

#### Execution parameters

Run with `python3 setup.py -hn <host number> -p <port>`.

- Example: `python3 setup.py -hn 0 -p 26257` (run setup on node at xcnc0.comp.nus.edu.sg:26257)

### client.py

Runs transactions given a transaction file, outputting metrics (to stderr) and transaction output (to stdout) when completed.

### Execution parameters

Run with `python3 client.py <transaction file> -hn <host number> -p <port>`

- Example : `python3 client.py xact-files/1.txt -hn 0 -p 26257` (run transactions in 1.txt on node at xcnc0.comp.nus.edu.sg:26257)

### output_state.py

Outputs the state of the database (15 statistics according to the project description) into the given file

### Execution parameters

Run with `python3 output_state.py <output file> -hn <host number> -p <port>`

- Example: `python3 output_state.py experiment1.out -hn 0 -p 26257` (output state as comma separated values into experiment1.out, querying node at xcnc0.comp.nus.edu.sg:26257)

### aggregate-metrics.py

Run with `python3 aggregate-metrics.py`. Output is an `all_metrics.csv` file in the root directory with the aggregated metrics (min, max, avg) of all performance benchmarks, grouped by experiment per row.

# Running an experiment

## Configuration

# Important files

## transaction.py

This file contains the implementations of the transactions needed to run the experiments.

## \*.sql

There are several `.sql` files in the root directory. Their uses are categorized as such:

### Setup

- `create-tables.sql`
- `drop-tables.sql`
- `load-data.sql`

### Transactions

- `popular-item.sql`
- `top-balance.sql`
- `related-customer.sql`
