package edu.uci.ics.tippers.config;

import java.util.Map;
// code site: https://www.opencodez.com/java/implement-custom-value-serializer-apache-kafka.htm
import org.apache.kafka.common.serialization.Serializer;

import edu.uci.ics.tippers.model.middleware.Message;

import com.fasterxml.jackson.databind.ObjectMapper;

public class MessageSerializer implements Serializer<Message> {

    @Override
    public byte[] serialize(String arg0, Message msg) {
        byte[] serializedBytes = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            serializedBytes = objectMapper.writeValueAsString(msg).getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return serializedBytes;
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
    }

    @Override
    public void configure(Map<String, ?> arg0, boolean arg1) {
        // TODO Auto-generated method stub
    }
}