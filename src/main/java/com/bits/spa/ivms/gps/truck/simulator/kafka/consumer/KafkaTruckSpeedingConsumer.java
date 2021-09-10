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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Properties;

@Component
public class KafkaTruckSpeedingConsumer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaTruckSpeedingConsumer.class);

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
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-fifth-application");
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
                            double speedInMPS = distanceMeters / (timeDiff/1000.0);
                            double speedInKMPH = speedInMPS * 3.6;
                            logger.info("Speeding Consumer: Distance (m): {} timeDiff (ms): {} speed (mps): {} speed (kmph): {}", distanceMeters, timeDiff, speedInMPS, speedInKMPH);
                        }
                    }
                }
            }
        }
    }

    @PreDestroy
    public void closeConnections() {
        kafkaConsumer.close();
    }
}
