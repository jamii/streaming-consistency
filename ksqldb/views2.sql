CREATE TABLE accepted_transactions WITH (
    kafka_topic='accepted_transactions',
    value_format='json',
    partitions=1
) AS
SELECT
    *
FROM
    transactions
EMIT CHANGES;

CREATE TABLE outer_join WITH (
    kafka_topic='outer_join',
    value_format='json',
    partitions=1
) AS
SELECT
    t1.id,
    t2.id as other_id,
FROM
    transactions as t1
LEFT JOIN
    transactions as t2
ON
    t1.id = t2.id
EMIT CHANGES;

CREATE TABLE credits WITH (
    kafka_topic='credits',
    value_format='json',
    partitions=1
) AS
SELECT
    to_account AS account, 
    sum(amount) AS credits
FROM
    transactions
GROUP BY
    to_account
EMIT CHANGES;

CREATE TABLE debits WITH (
    kafka_topic='debits',
    value_format='json',
    partitions=1
) AS
SELECT
    from_account AS account, 
    sum(amount) AS debits
FROM
    transactions
GROUP BY
    from_account
EMIT CHANGES;

CREATE TABLE balance WITH (
    kafka_topic='balance',
    value_format='json',
    partitions=1
) AS
SELECT
    credits.account AS account, 
    credits - debits AS balance
FROM
    credits 
INNER JOIN
    debits
ON
    credits.account = debits.account
EMIT CHANGES;

CREATE TABLE total WITH (
    kafka_topic='total',
    value_format='json',
    partitions=1
) AS
SELECT
    'foo',
    sum(balance)
FROM
    balance
GROUP BY
    'foo'
EMIT CHANGES;