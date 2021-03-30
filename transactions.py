#!/usr/bin/env python3

import random

max_id = 1000000
for id in range(1,max_id):
    second = ((50 * id) // max_id) + random.randint(0,9)
    print(f'{id} | {id}, {random.randint(0,9)}, {random.randint(0,9)}, {random.random()}, 2021-01-01 00:00:{second}')