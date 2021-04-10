import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

public class Demo {
  public static void main(String[] args) {
    String logFile = "/home/jamie/streaming-consistency/spark-structured-streaming/README.md"; // Should be some file on your system
    SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
    Dataset<String> logData = spark.read().textFile(logFile).cache();

    long numAs = logData.filter((org.apache.spark.api.java.function.FilterFunction<String>) s -> s.contains("a")).count();
    long numBs = logData.filter((org.apache.spark.api.java.function.FilterFunction<String>) s -> s.contains("b")).count();

    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

    spark.stop();
  }
}