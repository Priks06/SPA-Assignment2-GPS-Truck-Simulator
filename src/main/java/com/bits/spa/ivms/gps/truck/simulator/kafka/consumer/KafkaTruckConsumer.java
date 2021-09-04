package com.bits.spa.ivms.gps.truck.simulator.kafka.consumer;

import com.bits.spa.ivms.gps.truck.simulator.service.TruckData;
import com.bits.spa.ivms.gps.truck.simulator.service.TruckDataDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Component
public class KafkaTruckConsumer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaTruckConsumer.class);

    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServer = "10.128.175.151:9092";

    @Value("${kafka.topic}")
    private String kafkaTopic = "first_topic";

    private KafkaConsumer<Integer, TruckData> kafkaConsumer;

    @Async("streamEvenHandlerTaskExecutor")
    public void startConsumingStreamData() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TruckDataDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-fourth-application");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        kafkaConsumer = new KafkaConsumer<>(properties);

        kafkaConsumer.subscribe(Collections.singletonList(kafkaTopic));

        while (true) {
            ConsumerRecords<Integer, TruckData> records = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<Integer, TruckData> record : records) {
                if (record.value() != null)
                logger.info("Key: {} Value: {}", record.key(), record.value());
            }
        }
    }

    public static void main(String[] args) {
        KafkaTruckConsumer kafkaTruckConsumer = new KafkaTruckConsumer();
        kafkaTruckConsumer.startConsumingStreamData();
    }

    @PreDestroy
    public void closeConnections() {
        kafkaConsumer.close();
    }
}
