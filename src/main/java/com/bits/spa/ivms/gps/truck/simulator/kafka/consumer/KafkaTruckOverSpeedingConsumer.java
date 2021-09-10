package com.bits.spa.ivms.gps.truck.simulator.kafka.consumer;

import com.bits.spa.ivms.gps.truck.simulator.service.TruckSerdes;
import com.bits.spa.ivms.gps.truck.simulator.service.TruckSpeed;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.Properties;

@Component
public class KafkaTruckOverSpeedingConsumer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaTruckOverSpeedingConsumer.class);

    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServer;

    @Value("${kafka.speed.topic}")
    private String kafkaSpeedTopic;

    @Value("${kafka.over.speed.topic}")
    private String kafkaOverSpeedTopic;

    private KafkaStreams kafkaStreams;


    @Async("streamEvenHandlerTaskExecutor")
    public void filterAndPublishOverSpeedingTrucks() {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TruckSerdes.TruckSpeedSerde.class.getName());
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "my-sixth-application");

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, TruckSpeed> speedStream = streamsBuilder.stream(kafkaSpeedTopic);
        speedStream
                .filter((key, truckSpeed) -> Double.parseDouble(truckSpeed.getSpeed()) > 21.0)
                .to(kafkaOverSpeedTopic);

        kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
        logger.info("Starting Kafka Streams!!");
        kafkaStreams.start();
        logger.info("Started Kafka Streams!!");
    }

    @PreDestroy
    public void closeConnections() {
        kafkaStreams.close();
    }
}
