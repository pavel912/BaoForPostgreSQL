import argparse
import psycopg2
import os
import sys
import random
from time import time, sleep

PG_CONNECTION_STR = "dbname=imdb user=imdb host=localhost"


def explain_query(sql):
    conn = psycopg2.connect(PG_CONNECTION_STR)
    cur = conn.cursor()
    cur.execute("SET pg_bao.bao_host TO localhost")
    cur.execute(f"SET pg_bao.enable_bao TO {False}")
    cur.execute(f"SET pg_bao.enable_bao_selection TO {False}")
    cur.execute(f"SET pg_bao.enable_bao_rewards TO {False}")
    cur.execute("SET pg_bao.bao_num_arms TO 5")
    cur.execute("SET statement_timeout TO 300000")
    cur.execute("EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON) " + sql)
    query_plan = cur.fetchall()
    conn.close()
    return query_plan

query_paths = sys.argv[1:]
queries = []
for fp in query_paths:
    with open(fp) as f:
        query = f.read()
    queries.append((fp, query))

print("Explaining queries")

data = []

for query in queries:
    data.append([query[0], query[1], explain_query(query[1])])

with open("query_plans", "x") as f:
    for d in data[0:]:
        print(d)
        f.write("Query path: " + d[0] + "\n")
        f.write(d[1] + "\n")
        f.write(d[2][0][0] + "\n")
        f.write("\n")