package edu.uci.ics.tippers.config;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.ser.std.StringSerializer;

// import com.fasterxml.jackson.databind.deser.std.StringDeserializer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.messaging.MessageChannel;

// import edu.uci.ics.tippers.model.middleware.mget_obj;
import edu.uci.ics.tippers.dbms.MallData;
import edu.uci.ics.tippers.model.middleware.LeTime;
import edu.uci.ics.tippers.model.middleware.Message;
import edu.uci.ics.tippers.model.middleware.QueryKafka;
@EnableKafka
@Configuration
public class KafkaConfig {
    @Bean
    public ProducerFactory<String, Message> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"edu.uci.ics.tippers.config.MessageSerializer");
        return new DefaultKafkaProducerFactory<String,Message>(config);
        //<String, Message>
    }
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "group_id");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        

        return new DefaultKafkaConsumerFactory<String, String>(configs);
    }

    @Bean
    public ConsumerFactory<String, QueryKafka> queryKafkaConsumerFactory(){
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "group_json");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, QueryKafkaDeserializer.class);
        return new DefaultKafkaConsumerFactory<String, QueryKafka>(configs, new StringDeserializer(), new QueryKafkaDeserializer());
    }
    @Bean
    public ConsumerFactory<String, LeTime> leTimeConsumerFactory() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "group_json");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        return new DefaultKafkaConsumerFactory<String, LeTime>(configs, new StringDeserializer(), new JsonDeserializer<>(LeTime.class));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(){
        ConcurrentKafkaListenerContainerFactory<String, String>  factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, QueryKafka> queryKafkaKakfaListenerFactory(){
        ConcurrentKafkaListenerContainerFactory<String, QueryKafka>  factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(queryKafkaConsumerFactory());
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, LeTime> leTimeKafkaListenerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, LeTime> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(leTimeConsumerFactory());
        return factory;
    }
    @Bean
    public KafkaTemplate<String, Message> kafkaTemplate() {
        return new KafkaTemplate<String, Message>(producerFactory());
    }

}
