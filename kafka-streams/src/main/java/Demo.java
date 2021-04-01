/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.scattered_thoughts.streaming_consistency;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.util.Properties;

public class Demo {

    public static void main(final String[] args) throws Exception {

        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
        final Consumed<String, JsonNode> consumed = Consumed.with(Serdes.String(), jsonSerde);
        
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<String, JsonNode> transactions = builder.table("transactions", consumed);
        
        //transactions
            //.toStream().to("accepted_transactions", Produced.with(Serdes.String(), jsonSerde));
        
        //transactions
            //.leftJoin(transactions, (t1, t2) -> 
                //t2 == null ? "null" : t2.get("id").textValue())
            //.toStream().to("outer_join", Produced.with(Serdes.String(), Serdes.String()));
            
        //transactions
            //.groupBy((k,v) -> KeyValue.pair("yolo", v), Grouped.with(Serdes.String(), jsonSerde))
            //.aggregate(
                //() -> 0L,
                //(k, v, sum) -> sum + v.get("amount").longValue(),
                //(k, v, sum) -> sum - v.get("amount").longValue())
            //.toStream().to("sums", Produced.with(Serdes.String(), Serdes.Long()));
            
        KTable<Long, Long> credits = transactions
            .groupBy((k,v) -> KeyValue.pair(v.get("to_account").longValue(), v))
            .aggregate(
                () -> 0L,
                (k, v, sum) -> sum + v.get("amount").longValue(),
                (k, v, sum) -> sum - v.get("amount").longValue());
        KTable<Long, Long> debits = transactions
            .groupBy((k,v) -> KeyValue.pair(v.get("from_account").longValue(), v))
            .aggregate(
                () -> 0L,
                (k, v, sum) -> sum + v.get("amount").longValue(),
                (k, v, sum) -> sum - v.get("amount").longValue());
        KTable<Long, Long> balance = credits
            .join(debits, (c, d) -> c - d);
        balance
            .toStream().to("balance", Produced.with(Serdes.Long(), Serdes.Long()));
        balance
            .groupBy((k,v) -> KeyValue.pair("yolo", v), Grouped.with(Serdes.String(), Serdes.Long()))
            .aggregate(
                () -> 0L,
                (k, v, sum) -> sum + v,
                (k, v, sum) -> sum - v)
            .toStream().to("total", Produced.with(Serdes.String(), Serdes.Long()));
          
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "demo");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, JsonTimestampExtractor.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
