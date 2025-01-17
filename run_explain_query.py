import argparse
import psycopg2
import os
import sys
import random
from time import time, sleep

PG_CONNECTION_STR = "dbname=imdb user=imdb host=localhost"


def exaplain_query(sql):
    conn = psycopg2.connect(PG_CONNECTION_STR)
    cur = conn.cursor()
    cur.execute("SET pg_bao.bao_host TO localhost")
    cur.execute(f"SET pg_bao.enable_bao TO {False}")
    cur.execute(f"SET pg_bao.enable_bao_selection TO {False}")
    cur.execute(f"SET pg_bao.enable_bao_rewards TO {False}")
    cur.execute("SET pg_bao.bao_num_arms TO 5")
    cur.execute("SET statement_timeout TO 300000")
    cur.execute("EXPLAIN " + sql)
    query_plan = cur.fetchall()
    conn.close()
    return query_plan

query_path = sys.argv[1]

with open(query_path) as f:
    query = f.read()

print("Explaining query", query)

print(exaplain_query(query))
