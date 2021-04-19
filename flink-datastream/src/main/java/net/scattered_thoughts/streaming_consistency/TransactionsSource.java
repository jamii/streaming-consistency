package net.scattered_thoughts.streaming_consistency;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.*;
import java.sql.Timestamp;
import java.util.Arrays;

public class TransactionsSource implements SourceFunction<JsonNode> {

    private final String dataFilePath;
    private transient BufferedReader reader;
    private final int watermarkFrequency;

    public TransactionsSource(String dataFilePath, int watermarkFrequency) {
        this.dataFilePath = dataFilePath;
        this.watermarkFrequency = watermarkFrequency;
    }

    @Override
    public void run(SourceContext<JsonNode> sourceContext) throws Exception {

        File directory = new File(dataFilePath);
        File[] files = directory.listFiles();
        Arrays.sort(files);
        for (File file : files) {
            FileInputStream fis = new FileInputStream(file);
            reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
            generateStream(sourceContext);
            this.reader.close();
            this.reader = null;
        }
    }

    private void generateStream(SourceContext<JsonNode> sourceContext) throws Exception {

        int numEvents = 0;
        String line;
        JsonNode event;
        ObjectMapper mapper = new ObjectMapper();

        while (reader.ready() && (line = reader.readLine()) != null) {
            numEvents++;
            event = mapper.readTree(line);
            long timestamp = Timestamp.valueOf(event.get("ts").textValue()).getTime();
            sourceContext.collectWithTimestamp(event, timestamp);

            // generate watermark
            if (numEvents == watermarkFrequency) {
                Watermark nextWatermark = new Watermark(timestamp - 10000);
                sourceContext.emitWatermark(nextWatermark);
                numEvents = 0;
            }
        }
    }

    @Override
    public void cancel() {
        try {
            if (this.reader != null) {
                this.reader.close();
            }
        } catch (IOException ioe) {
            throw new RuntimeException("Could not cancel SourceFunction", ioe);
        } finally {
            this.reader = null;
        }
    }
}
