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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.*;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.*;

public class Demo {
    
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "demo");
        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer<>("transactions", new JsonDeserializationSchema(), properties);
        consumer.setStartFromEarliest();
        consumer.assignTimestampsAndWatermarks(
            WatermarkStrategy
                .forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, timestamp) -> {
                    JsonNode node = (JsonNode) event;
                    return Timestamp.valueOf(node.get("ts").textValue()).getTime();
                })
        );
        DataStream<JsonNode> transactions = env
            .addSource(consumer);
        sinkToKafka("accepted_transactions", transactions);
        
        DataStream<Tuple2<Long, Double>> debits = 
            transactions
                .map(new MapFunction<JsonNode, Tuple2<Long, Double>>() {
                    @Override
                    public Tuple2<Long, Double> map(JsonNode input) throws Exception {
                        return new Tuple2<Long, Double>(
                            input.get("from_account").asLong(),
                            -input.get("amount").asDouble()
                        );
                    }
                });
        sinkToKafka("debits", debits);
        
        DataStream<Tuple2<Long, Double>> credits = 
            transactions
                .map(new MapFunction<JsonNode, Tuple2<Long, Double>>() {
                    @Override
                    public Tuple2<Long, Double> map(JsonNode input) throws Exception {
                        return new Tuple2<Long, Double>(
                            input.get("to_account").asLong(),
                            input.get("amount").asDouble()
                        );
                    }
                });
        sinkToKafka("credits", credits);
        
        DataStream<Tuple2<Long, Double>> balance = 
            debits
                .union(credits)
                .keyBy(tuple -> tuple.f0)
                .sum(1);
        sinkToKafka("balance", balance);
        
        // TODO this is not correct
        DataStream<Double> total =
            balance
                .keyBy(tuple -> "the key")
                .sum(1)
                .map(tuple -> tuple.f1);
        sinkToKafka("total", total);
        
        env.execute("Demo");
    }
    
    public static void sinkToKafka(String topic, DataStream stream) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        FlinkKafkaProducer<String> sink = new FlinkKafkaProducer<String>(
            topic,                  
            new SimpleStringSchema(),  
            properties
        ); 
        stream
        .map(kv -> kv.toString())
        .addSink(sink);
    }
}
