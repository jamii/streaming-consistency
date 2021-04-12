package net.scattered_thoughts.streaming_consistency;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.expressions.*;
import org.apache.flink.table.functions.ScalarFunction;
import static org.apache.flink.table.api.Expressions.*;
import org.apache.flink.api.java.typeutils.*;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.types.*;
import java.sql.Timestamp;
import java.util.Properties;
import org.apache.flink.streaming.connectors.kafka.*;
import org.apache.flink.api.common.serialization.*;

public class Demo {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        tEnv.executeSql(String.join("\n",
            "CREATE TABLE transactions (",
            "    id  BIGINT,",
            "    from_account INT,",
            "    to_account INT,",
            "    amount DOUBLE,",
            "    ts TIMESTAMP(3),",
            "    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND",
            ") WITH (",  
            "    'connector' = 'filesystem',",
            "    'path' = '/home/jamie/streaming-consistency/flink/tmp/transactions',",
            "    'format' = 'json',",
            "    'json.fail-on-missing-field' = 'true',",
            "    'json.ignore-parse-errors' = 'false'",
            ")"
        ));
        
        tEnv.executeSql(String.join("\n",
            "CREATE VIEW accepted_transactions(id) AS",
            "SELECT",
            "    id",
            "FROM",
            "    transactions"
        ));
        
        tEnv.executeSql(String.join("\n",
            "CREATE VIEW outer_join_with_time(id, other_id) AS",
            "SELECT",
            "    t1.id, t2.id as other_id",
            "FROM",
            "    transactions as t1",
            "LEFT JOIN",
            "    transactions as t2",
            "ON",
            "    t1.id = t2.id AND t1.ts = t2.ts"
        ));
        tEnv.executeSql("CREATE TEMPORARY TABLE outer_join_with_time_sink(id BIGINT, other_id BIGINT) WITH ( 'connector' = 'filesystem', 'path' = '/home/jamie/streaming-consistency/flink/tmp/outer_join_with_time', 'format' = 'json' )");
        tEnv.sqlQuery("SELECT * FROM outer_join_with_time").executeInsert("outer_join_with_time_sink");
        
        tEnv.executeSql(String.join("\n",
            "CREATE VIEW outer_join_without_time(id, other_id) AS",
            "SELECT",
            "    t1.id, t2.id as other_id",
            "FROM",
            "    (SELECT id FROM transactions) as t1",
            "LEFT JOIN",
            "    (SELECT id FROM transactions) as t2",
            "ON",
            "    t1.id = t2.id"
        ));
        tEnv.executeSql("CREATE TEMPORARY TABLE outer_join_without_time_sink(id BIGINT, other_id BIGINT) WITH ( 'connector' = 'filesystem', 'path' = '/home/jamie/streaming-consistency/flink/tmp/outer_join_without_time', 'format' = 'json' )");
        tEnv.sqlQuery("SELECT * FROM outer_join_without_time").executeInsert("outer_join_without_time_sink");
        
        tEnv.executeSql(String.join("\n",
            "CREATE VIEW credits(account, credits) AS",
            "SELECT",
            "    to_account as account, sum(amount) as credits",
            "FROM",
            "    transactions",
            "GROUP BY",
            "    to_account"
        ));  
        tEnv.executeSql(String.join("\n",
            "CREATE VIEW debits(account, debits) AS",
            "SELECT",
            "    from_account as account, sum(amount) as debits",
            "FROM",
            "    transactions",
            "GROUP BY",
            "    from_account"
        ));
        tEnv.executeSql(String.join("\n",
            "CREATE VIEW balance(account, balance) AS",
            "SELECT",
            "    credits.account, credits - debits as balance",
            "FROM",
            "    credits, debits",
            "WHERE",
            "    credits.account = debits.account"
        ));
        tEnv.executeSql(String.join("\n",
            "CREATE VIEW total(total) AS",
            "SELECT",
            "    sum(balance)",
            "FROM",
            "    balance"
        ));
        tEnv.executeSql("CREATE TEMPORARY TABLE total_sink(total DOUBLE) WITH ( 'connector' = 'filesystem', 'path' = '/home/jamie/streaming-consistency/flink/tmp/total', 'format' = 'json' )");
        tEnv.sqlQuery("SELECT * FROM total").executeInsert("total_sink");
        
        tEnv.executeSql(String.join("\n",
            "CREATE VIEW credits2(account, credits, ts) AS",
            "SELECT",
            "    to_account as account, sum(amount) as credits, max(ts) as ts",
            "FROM",
            "    transactions",
            "GROUP BY",
            "    to_account"
        ));  
        tEnv.executeSql(String.join("\n",
            "CREATE VIEW debits2(account, debits, ts) AS",
            "SELECT",
            "    from_account as account, sum(amount) as debits, max(ts) as ts",
            "FROM",
            "    transactions",
            "GROUP BY",
            "    from_account"
        ));
        tEnv.executeSql(String.join("\n",
            "CREATE VIEW balance2(account, balance, ts) AS",
            "SELECT",
            "    credits2.account, credits - debits as balance, credits2.ts",
            "FROM",
            "    credits2, debits2",
            "WHERE",
            "    credits2.account = debits2.account AND credits2.ts = debits2.ts"
        ));
        tEnv.executeSql(String.join("\n",
            "CREATE VIEW total2(total) AS",
            "SELECT",
            "    sum(balance)",
            "FROM",
            "    balance2"
        ));
        tEnv.executeSql("CREATE TEMPORARY TABLE total2_sink(total DOUBLE) WITH ( 'connector' = 'filesystem', 'path' = '/home/jamie/streaming-consistency/flink/tmp/total2', 'format' = 'json' )");
        tEnv.sqlQuery("SELECT * FROM total2").executeInsert("total2_sink");
        
        // tEnv.execute("Demo");
    }
}
