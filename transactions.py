#!/usr/bin/env python3

import random
import json

random.seed(42)
max_id = 1000000
for id in range(0,max_id):
    second = ((50 * id) // max_id) + random.randint(0,9)
    row = json.dumps({
        'id': id,
        'from_account': random.randint(0,9),
        'to_account': random.randint(0,9),
        'amount': 1,
        'ts': f'2021-01-01 00:00:{second:02d}.000',
    })
    print(f'{id}|{row}')