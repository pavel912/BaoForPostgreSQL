import argparse
import psycopg2
import os
import sys
import random
from time import time, sleep

PG_CONNECTION_STR = "dbname=imdb user=imdb host=localhost"

# https://stackoverflow.com/questions/312443/
def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def run_query(sql, bao_select=False, bao_reward=False):
    start = time()
    while True:
        try:
            conn = psycopg2.connect(PG_CONNECTION_STR)
            cur = conn.cursor()
            cur.execute("SET pg_bao.bao_host TO localhost")
            cur.execute(f"SET pg_bao.enable_bao TO {bao_select or bao_reward}")
            cur.execute(f"SET pg_bao.enable_bao_selection TO {bao_select}")
            cur.execute(f"SET pg_bao.enable_bao_rewards TO {bao_reward}")
            cur.execute("SET pg_bao.bao_num_arms TO 5")
            cur.execute("SET statement_timeout TO 300000")
            cur.execute(q)
            cur.fetchall()
            conn.close()
            break
        except:
            sleep(1)
            continue
    stop = time()
    return stop - start

        
query_paths = sys.argv[1:]
queries = []
for fp in query_paths:
    with open(fp) as f:
        query = f.read()
    queries.append((fp, query))
print("Read", len(queries), "queries.")
print("Using Bao:", True)

random.seed(42)
query_sequence = random.choices(queries, k=50)
pg_chunks, *bao_chunks = list(chunks(query_sequence, 25))

print("Executing training workload")

for fp, q in pg_chunks:
    pg_time = run_query(q, bao_reward=True)
    print("x", "x", time(), fp, pg_time, "PG", flush=True)

for c_idx, chunk in enumerate(bao_chunks):
    os.system("python3 BaoForPostgreSQL/bao_server/baoctl.py --retrain")
    os.system("sync")

    for q_idx, (fp, q) in enumerate(chunk):
        q_time = run_query(q, bao_reward=True, bao_select=True)
        print(c_idx, q_idx, time(), fp, q_time, flush=True)
