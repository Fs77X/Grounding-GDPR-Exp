package edu.uci.ics.tippers.config;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import edu.uci.ics.tippers.model.middleware.QueryKafka;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.VisibilityChecker;
public class QueryKafkaDeserializer implements Deserializer<QueryKafka>{
    @Override
	public QueryKafka deserialize(String arg0, byte[] msgBytes) {
		ObjectMapper mapper = new ObjectMapper();
		mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
		mapper.setVisibility(VisibilityChecker.Std.defaultInstance().withFieldVisibility(JsonAutoDetect.Visibility.ANY));
		QueryKafka msg = null;
		try {
			msg = mapper.readValue(msgBytes, QueryKafka.class);
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


