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
import org.apache.flink.formats.csv.*;

public class Demo {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv, settings);

        tEnv.executeSql(String.join("\n",
            "CREATE TABLE transactions (",
            "    id  BIGINT,",
            "    from_account INT,",
            "    to_account INT,",
            "    amount DOUBLE,",
            "    ts TIMESTAMP(3),",
            "    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND",
            ") WITH (",  
            "    'connector' = 'kafka',",
            "    'topic' = 'transactions',",
            "    'properties.bootstrap.servers' = 'localhost:9092',",
            "    'properties.group.id' = 'demo',",
            "    'scan.startup.mode' = 'earliest-offset',",
            "    'format' = 'csv'",
            ")"
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
        sinkToKafka(tEnv, "outer_join_with_time");
        
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
        sinkToKafka(tEnv, "outer_join_without_time");
        
        //transactions.select(
            //$("id").as("other_id"),
            //$("ts").as("other_ts"))
        //.leftOuterJoin(
            //transactions
                //.select(
                    //call(Demo.SlowMap.class, $("id")).as("id"),
                    //$("ts")),
            //and(
                //$("id").isEqual($("other_id")),
                //$("other_ts").isEqual($("ts"))))
        //.select(
            //$("other_id"),
            //$("id"))
        //.executeInsert("outputs");
        
        //Table mean_of_square = 
            //transactions
            //.groupBy($("kind"))
            //.select(
                //$("kind"), 
                //$("score").power(lit(2)).avg().as("mean_of_square"));
        //Table square_of_mean = 
            //transactions
            //.groupBy($("kind"))
            //.select(
                //$("kind").as("other_kind"), 
                //$("score").avg().power(lit(2)).as("square_of_mean"));
        //mean_of_square
            //.join(
                //square_of_mean,
                //$("kind").isEqual($("other_kind")))
            //.select(
                //$("kind"),
                //$("mean_of_square").minus($("square_of_mean")).as("variance"),
                //$("mean_of_square").minus($("square_of_mean")).power(0.5).as("stddev"))
            //.executeInsert("outputs2");      
        
        // TODO need repeats per ts?
        // Table coarse_transactions = transactions.union(transactions).union(transactions);
        //transactions
            //.groupBy($("kind"))
            //.select(
                //$("kind"), 
                //$("score").power(lit(2)).avg().as("mean_of_square"),
                //$("score").avg().power(lit(2)).as("square_of_mean"))
            //.select(
                //$("kind"),
                //$("mean_of_square").minus($("square_of_mean")).as("variance"),
                //$("mean_of_square").minus($("square_of_mean")).power(0.5).as("stddev"))
            //.executeInsert("outputs2");
        
        //transactions
            //.groupBy($("kind"))
            //.select(
                //$("kind"),
                //$("score").power(lit(2)).avg().minus($("score").avg().power(lit(2))).as("variance2"),
                //$("score").power(lit(2)).avg().minus($("score").avg().power(lit(2))).power(0.5).as("stddev2"),
                //$("score").varPop().as("variance"),
                //$("score").stddevPop().as("stddev"))
            //.executeInsert("outputs2");
            
        //transactions.executeInsert("outputs3");
            
        //Table even = transactions
            //.filter($("id").mod(lit(2)).isEqual(lit(0)))
            //.groupBy($("kind"))
            //.select(
                //$("kind"), 
                //$("id").count().as("even_total"));
        //all.join(
            //even.select(
                //$("kind").as("other_kind"),
                //$("even_total")), 
             //$("kind").isEqual($("other_kind")))
            //.select(
                //$("kind"),
                //$("all_total").minus($("even_total")).as("total"))
            //.executeInsert("outputs2");
        
        //TupleTypeInfo<Tuple4<Long, Integer, Double, Timestamp>> tupleType = new TupleTypeInfo<>(
            //Types.LONG(),
            //Types.INT(),
            //Types.DOUBLE(),
            //Types.SQL_TIMESTAMP()
        //);
        //DataStream<Tuple4<Long, Integer, Double, Timestamp>> transactions_stream = tEnv.toAppendStream(transactions, tupleType);
        //
        //transactions_stream
          //.keyBy(row -> row.getField(1))
          //.sum(2)
          //.print();
          
        
      sEnv.execute("Demo");
    }
    
    public static void sinkToKafka(StreamTableEnvironment tEnv, String name) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        FlinkKafkaProducer<String> sink = new FlinkKafkaProducer<String>(
            name,                  
            new SimpleStringSchema(),  
            properties
        ); 
        tEnv
        .toRetractStream(tEnv.from(name), Row.class)
        .map(kv -> 
            (kv.getField(0) ? "insert" : "delete")
            + " "
            + kv.getField(1).toString()
        )
        .addSink(sink);
    }
    
    public static class SlowMap extends ScalarFunction {
        public Long eval(Long a) {
            // do something for ~10ms
            try {
                Thread.sleep(100);
            } catch (Exception e) {
            }
            return a;
        }
    }
}
