package edu.uci.ics.tippers.producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import edu.uci.ics.tippers.model.middleware.LogMessage;
import edu.uci.ics.tippers.model.middleware.Message;

import java.util.Properties;
import edu.uci.ics.tippers.config.LogSerializer;
public class LogResults {
    private static final String topic = "logResults";
    public void sendResults(LogMessage lr) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", LogSerializer.class);
    
        KafkaProducer<String, LogMessage> producer = new KafkaProducer<>(props);
        producer.send(new ProducerRecord<String, LogMessage>(topic, lr));
        producer.close();
    }
  
    
}
