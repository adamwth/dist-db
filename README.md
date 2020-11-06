# CS4224 Team A (CockroachDB)

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

#### Execution parameters
Run with `python3 client.py <transaction file> -hn <host number> -p <port>`

- Example : `python3 client.py xact-files/1.txt -hn 0 -p 26257` (run transactions in 1.txt on node at xcnc0.comp.nus.edu.sg:26257)

### output_state.py
Outputs the state of the database (15 statistics according to the project description) into the given file

#### Execution parameters
Run with `python3 output_state.py <output file> -hn <host number> -p <port>`

- Example: `python3 output_state.py experiment1.out -hn 0 -p 26257` (output state as comma separated values into experiment1.out, querying node at xcnc0.comp.nus.edu.sg:26257)

### aggregate-metrics.py

#### Execution parameters
Run with `python3 aggregate-metrics.py`. Output is an `all_metrics.csv` file in the root directory with the aggregated metrics (min, max, avg) of all performance benchmarks, grouped by experiment per row.

# Running an experiment
The `run-experiment.sh` script is used to run clients in parallel, each reading a corresponding transaction file and assigned hosts in a round robin manner

## Configuration
The number of host instances and their addresses can be configured in the `run-experiment.sh` script.
- Example (to run on 3 nodes at xcnc0.comp.nus.edu.sg:26257, xcnc1.comp.nus.edu.sg:26257, xcnc2.comp.nus.edu.sg:26257):
```
####### CONFIGURATION OF MACHINES ##############
hosts[0]=0
ports[0]=26257
hosts[1]=1
ports[1]=26257
hosts[2]=2
ports[2]=26257
################################################
```

The `run-experiment.sh` script takes in an argument indicating the number of clients to run.
It expects all transaction files to be under `xact-files/` in the same directory. 

For each client number {i}, it will call `client.py` with {i}.txt and output stdout to {i}\_output.out and stderr to {i}\_stats.out. 
Additionally, a comma separated values form of metrics is saved to {i}.metrics.

## Procedure
0. Adjust configuration of `run-experiment.sh`
1. Reset state of database by running `setup.py`
    - `python3 setup.py -hn <host number> -p <port>`
2. Run transactions on clients in parallel
    - `./run-experiment.sh <number of clients>`
3. Wait till all clients have completed (all python3 processes spawned by `run-experiment.sh` have terminated)
4. Retrieve database state with `output_state.py`
    - `python3 output_state.py <output file name> -hn <host number> -p <port>`

## Outputs
In the same folder as `run-experiment.sh`, you should see the following files per client ({i} from 1 to num clients):
- {i}\_output.out: Output of transactions
- {i}\_stats.out: Metrics of client and any transactions that were retried excessively
- {i}.metrics: Metrics of client in comma separated values form

## Analyzing output

# Other Important Files

## transaction.py

This file contains the implementations of the transactions needed to run the experiments.

## \*.sql

There are several `.sql` files in the root directory. Their uses are categorized as such:

## test-xact-files/*.txt
Contains single transactions of each type to test client implementation.

### Setup

- `create-tables.sql`
- `drop-tables.sql`
- `load-data.sql`

### Transactions

- `popular-item.sql`
- `top-balance.sql`
- `related-customer.sql`
