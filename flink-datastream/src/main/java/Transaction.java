package net.scattered_thoughts.streaming_consistency;

import java.sql.Timestamp;

public class Transaction {
    Long id;
    Long from_account;
    Long to_account;
    Double amount;
    Timestamp ts;
    
    public Transaction(
        Long id2,
        Long from_account2,
        Long to_account2,
        Double amount2,
        Timestamp ts2
    ) {
        id = id2;
        from_account = from_account2;
        to_account = to_account2;
        amount = amount2;
        ts = ts2;
    }
}