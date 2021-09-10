package com.bits.spa.ivms.gps.truck.simulator.kafka.consumer;

import com.bits.spa.ivms.gps.truck.simulator.mongodb.TruckSpeedEntity;
import com.bits.spa.ivms.gps.truck.simulator.mongodb.TruckSpeedRepository;
import com.bits.spa.ivms.gps.truck.simulator.service.TruckData;
import com.bits.spa.ivms.gps.truck.simulator.service.TruckDataDeserializer;
import com.bits.spa.ivms.gps.truck.simulator.service.TruckSpeed;
import com.bits.spa.ivms.gps.truck.simulator.service.TruckSpeedSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Properties;

@Component
public class KafkaTruckSpeedingConsumer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaTruckSpeedingConsumer.class);

    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServer;

    @Value("${kafka.topic}")
    private String kafkaTopic;

    @Value("${kafka.speed.topic}")
    private String kafkaSpeedTopic;

    private KafkaConsumer<Integer, TruckData> kafkaConsumer;

    KafkaProducer<String, TruckSpeed> kafkaProducer;

    private final TruckSpeedRepository truckSpeedRepository;

    public KafkaTruckSpeedingConsumer(TruckSpeedRepository truckSpeedRepository) {
        this.truckSpeedRepository = truckSpeedRepository;
    }

    @Async("streamEvenHandlerTaskExecutor")
    public void startConsumingStreamData() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TruckDataDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-fifth-application");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        kafkaConsumer = new KafkaConsumer<>(properties);

        kafkaConsumer.subscribe(Collections.singletonList(kafkaTopic));

        createKafkaProducer();

        int maxEmptyRecordsCount = 2000;
        int noRecordsReadCount = 0;

        while (true) {
            ConsumerRecords<Integer, TruckData> records = kafkaConsumer.poll(Duration.ofMillis(500));
            if (records.count() == 0) {
                noRecordsReadCount++;
                if (noRecordsReadCount > maxEmptyRecordsCount) {
                    logger.info("Breaking consumer!!");
                    break;
                } else
                    continue;
            }
            for (ConsumerRecord<Integer, TruckData> record : records) {
                TruckData truckData = record.value();
                if (truckData != null) {
                    logger.info("Key: {} Value: {}", record.key(), truckData);
                    // Calculate speed of the vehicle
                    if (truckData.getPrevTimestamp() != null && truckData.getPrevLatitude() != null && truckData.getPrevLongitude() != null) {
                        double prevLatitude = Double.parseDouble(truckData.getPrevLatitude());
                        double prevLongitude = Double.parseDouble(truckData.getPrevLongitude());
                        double currLatitude = Double.parseDouble(truckData.getCurrLatitude());
                        double currLongitude = Double.parseDouble(truckData.getCurrLongitude());
                        double distanceMeters = DistanceCalculatorService.haversine(prevLatitude, prevLongitude, currLatitude, currLongitude) * 1000;
                        LocalDateTime prevTimestamp = LocalDateTime.from(DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(truckData.getPrevTimestamp()));
                        LocalDateTime currTimestamp = LocalDateTime.from(DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(truckData.getCurrTimestamp()));
                        long timeDiff = currTimestamp.toInstant(ZoneOffset.UTC).toEpochMilli() - prevTimestamp.toInstant(ZoneOffset.UTC).toEpochMilli();
                        if (timeDiff > 0) {
                            double speedInMPS = distanceMeters / (timeDiff / 1000.0);
                            double speedInKMPH = speedInMPS * 3.6;
                            logger.info("Speeding Consumer: Distance (m): {} timeDiff (ms): {} speed (mps): {} speed (kmph): {}", distanceMeters, timeDiff, speedInMPS, speedInKMPH);
                            saveTruckSpeedToMongo(truckData, speedInKMPH);
                            publishTruckSpeed(truckData, speedInKMPH);
                        }
                    }
                }
            }
        }
    }

    private void publishTruckSpeed(TruckData truckData, double speedInKMPH) {
        TruckSpeed truckSpeed = new TruckSpeed(truckData.getDriverId(), truckData.getRouteName(), truckData.getCurrTimestamp(), String.valueOf(speedInKMPH));
        ProducerRecord<String, TruckSpeed> record = new ProducerRecord<>(kafkaSpeedTopic, truckSpeed);
        logger.info("Kafka speed producer record: {}", record.value());
        kafkaProducer.send(record, (recordMetadata, e) -> {
            if (e == null) {
                // Successfully sent
                logger.info("Topic: {} __ Partition: {} __ Offset: {}", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
            } else {
                logger.error("Something went wrong while publishing data to Kafka. ", e);
            }
        });
    }

    private void createKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, TruckSpeedSerializer.class.getName());

        kafkaProducer = new KafkaProducer<>(properties);
    }

    private void saveTruckSpeedToMongo(TruckData truckData, double speedInKMPH) {
        TruckSpeedEntity truckSpeedEntity = new TruckSpeedEntity();
        truckSpeedEntity.setDriverId(truckData.getDriverId());
        truckSpeedEntity.setRouteName(truckData.getRouteName());
        truckSpeedEntity.setSpeed(String.valueOf(speedInKMPH));
        truckSpeedEntity.setTimestamp(truckData.getCurrTimestamp());

        truckSpeedRepository.save(truckSpeedEntity);
    }

    @PreDestroy
    public void closeConnections() {
        kafkaConsumer.close();
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
