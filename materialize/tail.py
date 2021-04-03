#!/usr/bin/env python3.8

import psycopg2
import sys

dsn = "postgresql://materialize@localhost:6875/materialize?sslmode=disable"
conn = psycopg2.connect(dsn)

table = sys.argv[1]

with conn.cursor() as cur:
    cur.execute(f'DECLARE c CURSOR FOR TAIL {table}')
    while True:
        cur.execute("FETCH ALL c")
        for row in cur:
            print(row)