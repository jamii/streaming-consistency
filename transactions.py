#!/usr/bin/env python3

import random

random.seed(42)
max_id = 100000
for id in range(0,max_id):
    second = ((50 * id) // max_id) + random.randint(0,9)
    print(f'{id} | {id}, {random.randint(0,9)}, {random.randint(0,9)}, 1, 2021-01-01 00:00:{second}')