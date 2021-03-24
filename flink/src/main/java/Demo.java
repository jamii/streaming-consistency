package net.scattered_thoughts.streaming_consistency;;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.expressions.TimeIntervalUnit;

import static org.apache.flink.table.api.Expressions.*;

public class Demo {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        tEnv.executeSql("CREATE TABLE inputs (\n" +
                "    id  BIGINT,\n" +
                "    transaction_time TIMESTAMP(3),\n" +
                "    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'datagen'\n" +
                ")");

        tEnv.executeSql("CREATE TABLE outputs (\n" +
                "    other_id BIGINT,\n" +
                "    id BIGINT,\n" +
                "    PRIMARY KEY (other_id) NOT ENFORCED" +
                ") WITH (\n" +
                "    'connector' = 'print'\n" +
                ")");

        Table inputs = tEnv.from("inputs");
        Table timeless_inputs = inputs.select(
            $("id"));
        timeless_inputs.select(
            $("id").as("other_id"))
        .leftOuterJoin(
            timeless_inputs,
            $("id").isEqual($("other_id")))
        .select(
            $("other_id"),
            $("id"))
        .executeInsert("outputs");
    }
}
