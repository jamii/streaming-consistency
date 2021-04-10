import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

public class Demo {
    public static void main(String[] args) throws Exception {
        
        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
        
        Dataset df = spark
        .readStream()
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "transactions")
        .option("startingOffsets", "earliest")
        .load();
        
        df
        .writeStream()
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "accepted_transactions")
        .option("checkpointLocation", "./tmp/")
        .start();
        
        spark.stop();
    }
}