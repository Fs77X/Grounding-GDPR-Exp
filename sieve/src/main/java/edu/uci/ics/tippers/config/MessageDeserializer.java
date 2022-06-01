package edu.uci.ics.tippers.config;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import edu.uci.ics.tippers.model.middleware.Message;

import com.fasterxml.jackson.databind.ObjectMapper;

public class MessageDeserializer implements Deserializer<Message> {
    @Override
	public Message deserialize(String arg0, byte[] msgBytes) {
		ObjectMapper mapper = new ObjectMapper();
		Message msg = null;
		try {
			msg = mapper.readValue(msgBytes, Message.class);
		} catch (Exception e) {

			e.printStackTrace();
		}
		return msg;
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
