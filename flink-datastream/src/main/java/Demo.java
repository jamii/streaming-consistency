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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class Demo {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        DataStream<Transaction> transactions = env
            .addSource(new FlinkKafkaConsumer<>("transactions", new TransactionDeserializationSchema(), properties));

            
        sinkToKafka("accepted_transactions", transactions);
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
