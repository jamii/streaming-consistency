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
            "CREATE TABLE inputs (",
            "    id  BIGINT,",
            "    kind SMALLINT,",
            "    score DOUBLE,",
            "    ts TIMESTAMP(3),",
            "    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND",
            ") WITH (",  
            "    'connector' = 'kafka',",
            "    'topic' = 'inputs',",
            "    'properties.bootstrap.servers' = 'localhost:9092',",
            "    'properties.group.id' = 'demo',",
            "    'scan.startup.mode' = 'earliest-offset',",
            "    'format' = 'csv'",
            ")"
        ));

        tEnv.executeSql(String.join("\n",
            "CREATE TABLE outputs (",
            "    other_id BIGINT,",
            "    id BIGINT",
            ") WITH (",
            "    'connector' = 'kafka',",
            "    'topic' = 'outputs',",
            "    'properties.bootstrap.servers' = 'localhost:9092',",
            "    'format' = 'csv'",
            ")"
        ));

        Table inputs = tEnv.from("inputs");
        
        Table timeless_inputs = inputs.select(
            $("id"));
        Table outputs = timeless_inputs.select(
            $("id").as("other_id"))
        .leftOuterJoin(
            timeless_inputs,
            $("id").isEqual($("other_id")))
        .select(
            $("other_id"),
            $("id"));
        
        //inputs.select(
            //$("id").as("other_id"),
            //$("ts").as("other_ts"))
        //.leftOuterJoin(
            //inputs
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
            //inputs
            //.groupBy($("kind"))
            //.select(
                //$("kind"), 
                //$("score").power(lit(2)).avg().as("mean_of_square"));
        //Table square_of_mean = 
            //inputs
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
        // Table coarse_inputs = inputs.union(inputs).union(inputs);
        //inputs
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
        
        //inputs
            //.groupBy($("kind"))
            //.select(
                //$("kind"),
                //$("score").power(lit(2)).avg().minus($("score").avg().power(lit(2))).as("variance2"),
                //$("score").power(lit(2)).avg().minus($("score").avg().power(lit(2))).power(0.5).as("stddev2"),
                //$("score").varPop().as("variance"),
                //$("score").stddevPop().as("stddev"))
            //.executeInsert("outputs2");
            
        //inputs.executeInsert("outputs3");
            
        //Table even = inputs
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
        //DataStream<Tuple4<Long, Integer, Double, Timestamp>> inputs_stream = tEnv.toAppendStream(inputs, tupleType);
        //
        //inputs_stream
          //.keyBy(row -> row.getField(1))
          //.sum(2)
          //.print();
          
      sinkToKafka(tEnv, outputs, "outputs");
        
      sEnv.execute("Demo");
    }
    
    public static void sinkToKafka(StreamTableEnvironment tEnv, Table table, String topic) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        FlinkKafkaProducer<String> sink = new FlinkKafkaProducer<String>(
            topic,                  
            new SimpleStringSchema(),  
            properties
        ); 
        tEnv
        .toRetractStream(table, Row.class)
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
