package edu.uci.ics.tippers.config;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import edu.uci.ics.tippers.model.middleware.PuciLog;
import java.util.Map;
public class PuciLogSerializer  implements Serializer<PuciLog> {

    @Override
    public byte[] serialize(String arg0, PuciLog msg) {
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
