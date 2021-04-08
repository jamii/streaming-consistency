package net.scattered_thoughts.streaming_consistency;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

public class JsonDeserializationSchema implements DeserializationSchema<JsonNode> {

    private static final long serialVersionUID = 1L;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public JsonNode deserialize(byte[] message) throws IOException {
        return objectMapper.readTree(message);
    }

    @Override
    public boolean isEndOfStream(JsonNode nextElement) {
        return false;
    }

    @Override
    public TypeInformation<JsonNode> getProducedType() {
        return TypeInformation.of(JsonNode.class);
    }
}