package net.scattered_thoughts.streaming_consistency;

import org.apache.flink.table.api.*;
import org.apache.flink.table.expressions.*;
import org.apache.flink.table.functions.ScalarFunction;
import static org.apache.flink.table.api.Expressions.*;

public class Demo {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        tEnv.executeSql(
                "CREATE TABLE inputs (\n" +
                "    id  BIGINT,\n" +
                "    kind SMALLINT,\n" +
                "    score SMALLINT,\n" +
                "    ts TIMESTAMP(3),\n" +
                "    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'datagen',\n" +
                "    'fields.id.kind' = 'sequence',\n" +
                "    'fields.id.start' = '0',\n" +
                "    'fields.id.end' = '1000000',\n" +
                "    'fields.kind.min' = '0',\n" +
                //"    'fields.kind.max' = '9',\n" +
                "    'fields.score.min' = '0',\n" +
                "    'fields.score.max' = '1'\n" +
                ")");

        tEnv.executeSql(
                "CREATE TABLE outputs (\n" +
                "    other_id BIGINT,\n" +
                "    id BIGINT,\n" +
                "    PRIMARY KEY (other_id) NOT ENFORCED" +
                ") WITH (\n" +
                "    'connector' = 'print'\n" +
                ")"
            );
         
        tEnv.executeSql(
                "CREATE TABLE outputs2 (\n" +
                "    kind SMALLINT,\n" +
                "    variance DOUBLE,\n" +
                "    stddev DOUBLE,\n" +
                "    PRIMARY KEY (kind) NOT ENFORCED" +
                ") WITH (\n" +
                "    'connector' = 'print'\n" +
                ")"
            );

        Table inputs = tEnv.from("inputs");
        
        //Table timeless_inputs = inputs.select(
            //$("id"));
        //timeless_inputs.select(
            //$("id").as("other_id"))
        //.leftOuterJoin(
            //timeless_inputs,
            //$("id").isEqual($("other_id")))
        //.select(
            //$("other_id"),
            //$("id"))
        //.executeInsert("outputs");
        
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
        
        Table mean_of_square = 
            inputs
            .groupBy($("kind"))
            .select(
                $("kind"), 
                $("score").power(lit(2)).avg().as("mean_of_square"));
        Table square_of_mean = 
            inputs
            .groupBy($("kind"))
            .select(
                $("kind").as("other_kind"), 
                $("score").avg().power(lit(2)).as("square_of_mean"));
        mean_of_square
            .join(
                square_of_mean,
                $("kind").isEqual($("other_kind")))
            .select(
                $("kind"),
                $("mean_of_square").minus($("square_of_mean")).as("variance"),
                $("mean_of_square").minus($("square_of_mean")).power(0.5).as("stddev"))
            .executeInsert("outputs2");      
        
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
                //$("score").avg().as("variance"),
                //$("score").stddevPop().as("stddev"))
            //.executeInsert("outputs2");
            
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
        
        // TODO no outer joins on temporal tables?
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
