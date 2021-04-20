package net.scattered_thoughts.streaming_consistency;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class Demo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<JsonNode> transactions = env
                .addSource(new TransactionsSource("./tmp/transactions", 100));

        // timestamp - from_account - amount_total
        DataStream<Tuple3<Long, Long, Double>> debits =
                transactions
                        .map(new ToDebit())
                        .keyBy(new GetAccount())
                        .process(new ProcessTransaction());

        debits.writeAsText("./tmp/debits").setParallelism(1);

        // timestamp - to_account - amount_total
        DataStream<Tuple3<Long, Long, Double>> credits =
                transactions
                        .map(new ToCredit())
                        .keyBy(new GetAccount())
                        .process(new ProcessTransaction());

        credits.writeAsText("./tmp/credits").setParallelism(1);

        // timestamp - account - balance
        DataStream<Tuple3<Long, Long, Double>> balance =
                debits
                        .keyBy(new GetAccount())
                        .union(credits)
                        .keyBy(new GetAccount())
                        .process(new ProcessTransaction());

        balance.writeAsText("./tmp/balance").setParallelism(1);

        DataStream<Tuple3<Long, Long, Double>> total =
                balance
                        .keyBy(new GetOneKey())
                        .process(new ProcessTotal()).setParallelism(1);

        total.writeAsText("./tmp/total").setParallelism(1);

        env.execute("Demo");
    }

    static final class ProcessTransaction extends KeyedProcessFunction<Long, Tuple3<Long, Long, Double>, Tuple3<Long, Long, Double>> {

        private MapState<Long, Double> balance; // timestamp -> amount

        @Override
        public void open(Configuration parameters) {
            MapStateDescriptor<Long, Double> taskDescriptor =
                    new MapStateDescriptor<>("amountState", Long.class, Double.class);
            balance = getRuntimeContext().getMapState(taskDescriptor);
        }

        @Override
        public void processElement(Tuple3<Long, Long, Double> in, Context context, Collector<Tuple3<Long, Long, Double>> collector) throws Exception {

            // add update map and set timer
            long timestamp = context.timestamp();
            double amount;
            if (balance.contains(timestamp)) {
                amount = balance.get(timestamp) + in.f2;
            }
            else {
                amount = in.f2;
            }
            balance.put(timestamp, amount);
            context.timerService().registerEventTimeTimer(context.timestamp());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple3<Long, Long, Double>> out) throws Exception {
            // emit updates
            out.collect(Tuple3.of(timestamp, ctx.getCurrentKey(), balance.get(timestamp)));
            // clean up state
            balance.remove(timestamp);
        }
    }

    static final class ProcessTotal extends KeyedProcessFunction<Long, Tuple3<Long, Long, Double>, Tuple3<Long, Long, Double>> {
        private ValueState<Double> total;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Double> taskDescriptor = new ValueStateDescriptor<>("totalState", Double.class);
            total = getRuntimeContext().getState(taskDescriptor);
        }

        @Override
        public void processElement(Tuple3<Long, Long, Double> in, Context context, Collector<Tuple3<Long, Long, Double>> out) throws IOException {

            double amount = 0;
            if (total.value() != null) {
                amount = total.value();
            }
            amount += in.f2;
            total.update(amount);
            context.timerService().registerEventTimeTimer(in.f0);
        }
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple3<Long, Long, Double>> out) throws IOException {
            // emit updates
            out.collect(Tuple3.of(timestamp, ctx.getCurrentKey(), total.value()));
        }
    }

    // project fields for debits
    static final class ToDebit implements MapFunction<JsonNode, Tuple3<Long, Long, Double>> {
        @Override
        public Tuple3<Long, Long, Double> map(JsonNode in) {
            return Tuple3.of(in.get("ts").asLong(),  in.get("from_account").asLong(), -in.get("amount").asDouble());
        }
    }

    // project fields for credits
    static final class ToCredit implements MapFunction<JsonNode, Tuple3<Long, Long, Double>> {
        @Override
        public Tuple3<Long, Long, Double> map(JsonNode in) {
            return Tuple3.of(in.get("ts").asLong(),  in.get("to_account").asLong(), in.get("amount").asDouble());
        }
    }

    static final class GetAccount implements KeySelector<Tuple3<Long, Long, Double>, Long> {
        @Override
        public Long getKey(Tuple3<Long, Long, Double> in) {
            return in.f1;
        }
    }

    static final class GetOneKey implements KeySelector<Tuple3<Long, Long, Double>, Long> {
        @Override
        public Long getKey(Tuple3<Long, Long, Double> in) throws Exception {
            return 0L;
        }
    }
}
