CREATE MATERIALIZED VIEW transactions_without_time AS
SELECT
    CAST(data->'id' AS INT) as id,
    CAST(data->'from_account' AS INT) as from_account,
    CAST(data->'to_account' AS INT) as to_account,
    CAST(data->'amount' AS DOUBLE) as amount,
    CAST(CAST(data->'ts' AS TEXT) AS TIMESTAMP) as ts
FROM (
    SELECT CAST(convert_from(data, 'utf8') AS jsonb) AS data
    FROM transactions_source
);

CREATE MATERIALIZED VIEW transactions AS
SELECT
    *
FROM
    transactions_without_time
WHERE
    (CAST(EXTRACT(EPOCH FROM ts) AS NUMERIC) * 10000) <= mz_logical_timestamp();

CREATE MATERIALIZED VIEW accepted_transactions AS
SELECT
    id
FROM
    transactions;

CREATE MATERIALIZED VIEW outer_join AS
SELECT
    t1.id AS id, 
    t2.id AS other_id
FROM
    (SELECT id FROM transactions) AS t1
LEFT JOIN
    (SELECT id FROM transactions) AS t2
ON
    t1.id = t2.id;

CREATE MATERIALIZED VIEW credits AS
SELECT
    to_account AS account, 
    sum(amount) AS credits
FROM
    transactions
GROUP BY
    to_account;

CREATE MATERIALIZED VIEW debits AS
SELECT
    from_account AS account, 
    sum(amount) AS debits
FROM
    transactions
GROUP BY
    from_account;

CREATE MATERIALIZED VIEW balance AS
SELECT
    credits.account AS account, 
    credits - debits AS balance
FROM
    credits,
    debits
WHERE
    credits.account = debits.account;

CREATE MATERIALIZED VIEW total AS
SELECT
    sum(balance)
FROM
    balance;