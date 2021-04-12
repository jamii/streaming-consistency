CREATE TABLE transactions (
    id VARCHAR PRIMARY KEY,
    from_account INT,
    to_account INT,
    amount DOUBLE,
    ts VARCHAR
) WITH (
    kafka_topic='transactions', 
    value_format='json', 
    partitions=1,
    timestamp='ts',
    timestamp_format='yyyy-MM-dd HH:mm:ss.SSS'
);