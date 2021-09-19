package com.bits.spa.ivms.gps.truck.simulator.boot;

import com.bits.spa.ivms.gps.truck.simulator.kafka.consumer.KafkaTruckMongoConsumer;
import com.bits.spa.ivms.gps.truck.simulator.kafka.consumer.KafkaTruckOverSpeedingConsumer;
import com.bits.spa.ivms.gps.truck.simulator.kafka.consumer.KafkaTruckSpeedingConsumer;
import com.bits.spa.ivms.gps.truck.simulator.mongodb.TruckDataRepository;
import com.bits.spa.ivms.gps.truck.simulator.publisher.MQTTPublisher;
import com.bits.spa.ivms.gps.truck.simulator.subscriber.MQTTSubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@ComponentScan(basePackages = "com.bits.spa.ivms.gps.truck.simulator")
@PropertySource("classpath:application.properties")
@EnableAsync
@EnableMongoRepositories(basePackageClasses = TruckDataRepository.class)
public class GPSSimulatorSpringBootApplication implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(GPSSimulatorSpringBootApplication.class);

    private final MQTTPublisher mqttPublisher;

    private final MQTTSubscriber mqttSubscriber;

    private final KafkaTruckMongoConsumer kafkaTruckMongoConsumer;

    private final KafkaTruckSpeedingConsumer kafkaTruckSpeedingConsumer;

    private final KafkaTruckOverSpeedingConsumer kafkaTruckOverSpeedingConsumer;

    public GPSSimulatorSpringBootApplication(MQTTPublisher mqttPublisher, MQTTSubscriber mqttSubscriber, KafkaTruckMongoConsumer kafkaTruckMongoConsumer, KafkaTruckSpeedingConsumer kafkaTruckSpeedingConsumer, KafkaTruckOverSpeedingConsumer kafkaTruckOverSpeedingConsumer) {
        this.mqttPublisher = mqttPublisher;
        this.mqttSubscriber = mqttSubscriber;
        this.kafkaTruckMongoConsumer = kafkaTruckMongoConsumer;
        this.kafkaTruckSpeedingConsumer = kafkaTruckSpeedingConsumer;
        this.kafkaTruckOverSpeedingConsumer = kafkaTruckOverSpeedingConsumer;
    }

    public static void main(String[] args) {
        SpringApplication.run(GPSSimulatorSpringBootApplication.class, args);
    }

    @Override
    public void run(String... args) {

        String brokerAddr = "tcp://test.mosquitto.org:1883";
        String topic = "spa/assignment2/geo_location";

        try {
            kafkaTruckOverSpeedingConsumer.filterAndPublishOverSpeedingTrucks();
            mqttPublisher.publishMessages(brokerAddr, topic);
            mqttSubscriber.listenToMessages(brokerAddr, topic);
            kafkaTruckMongoConsumer.startConsumingStreamData();
            kafkaTruckSpeedingConsumer.startConsumingStreamData();
        } catch (Exception e) {
            logger.error("Something went horribly wrong, ", e);
        }

    }

}
