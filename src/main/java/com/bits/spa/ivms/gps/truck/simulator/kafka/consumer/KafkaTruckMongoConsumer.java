package com.bits.spa.ivms.gps.truck.simulator.kafka.consumer;

import com.bits.spa.ivms.gps.truck.simulator.mongodb.TruckDataEntity;
import com.bits.spa.ivms.gps.truck.simulator.mongodb.TruckDataRepository;
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
public class KafkaTruckMongoConsumer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaTruckMongoConsumer.class);

    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServer = "10.128.175.151:9092";

    @Value("${kafka.topic}")
    private String kafkaTopic = "first_topic";

    private final TruckDataRepository truckDataRepository;

    private KafkaConsumer<Integer, TruckData> kafkaConsumer;

    public KafkaTruckMongoConsumer(TruckDataRepository truckDataRepository) {
        this.truckDataRepository = truckDataRepository;
    }

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

        int maxEmptyRecordsCount = 2000;
        int noRecordsReadCount = 0;

        while (true) {
            ConsumerRecords<Integer, TruckData> records = kafkaConsumer.poll(Duration.ofMillis(500));
            if (records.count() == 0) {
                noRecordsReadCount++;
                if (noRecordsReadCount >  maxEmptyRecordsCount) {
                    logger.info("Breaking consumer!!");
                    break;
                }
                else
                    continue;
            }
            for (ConsumerRecord<Integer, TruckData> record : records) {
                TruckData truckData = record.value();
                if (truckData != null) {
                    logger.info("Key: {} Value: {}", record.key(), truckData);
                    // Insert the raw data into MongoDB
                    TruckDataEntity truckDataEntity = new TruckDataEntity();
                    truckDataEntity.setDriverId(truckData.getDriverId());
                    truckDataEntity.setRouteName(truckData.getRouteName());
                    truckDataEntity.setTimestamp(truckData.getCurrTimestamp());
                    truckDataEntity.setLatitude(truckData.getCurrLatitude());
                    truckDataEntity.setLongitude(truckData.getCurrLongitude());
                    truckDataRepository.save(truckDataEntity);
                }
            }
        }
    }

    @PreDestroy
    public void closeConnections() {
        kafkaConsumer.close();
    }
}
