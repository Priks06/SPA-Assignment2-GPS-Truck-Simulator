package com.bits.spa.ivms.gps.truck.simulator.boot;

import com.bits.spa.ivms.gps.truck.simulator.publisher.MQTTPublisher;
import com.bits.spa.ivms.gps.truck.simulator.subscriber.MQTTSubscriber;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.PropertySource;
import org.springframework.scheduling.annotation.EnableAsync;

import java.util.UUID;

@SpringBootApplication
@ComponentScan(basePackages = "com.bits.spa.ivms.gps.truck.simulator")
@PropertySource("classpath:application.properties")
@EnableAsync
public class GPSSimulatorSpringBootApplication implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(GPSSimulatorSpringBootApplication.class);

    private final MQTTPublisher publisher;

    private final MQTTSubscriber subscriber;

    public GPSSimulatorSpringBootApplication(MQTTPublisher publisher, MQTTSubscriber subscriber) {
        this.publisher = publisher;
        this.subscriber = subscriber;
    }

    public static void main(String[] args) {
        SpringApplication.run(GPSSimulatorSpringBootApplication.class, args);
    }

    @Override
    public void run(String... args) {

//        String brokerAddr = "tcp://iot.eclipse.org:1883";
        String brokerAddr = "tcp://test.mosquitto.org:1883";
        String topic = "spa/assignment2/truck";

        try {
            publisher.publishMessages(brokerAddr, topic);
            subscriber.listenToMessages(brokerAddr, topic);
//            connectClientToBroker(createMQTTPublisher(brokerAddr));
        } catch (Exception e) {
            logger.error("Something went horribly wrong, ", e);
        }
    }

    private void connectClientToBroker(MqttClient publisher) throws MqttException {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);
        options.setConnectionTimeout(20);
        publisher.connect(options);
        logger.info("Connected: {}", publisher.isConnected());
    }

    public MqttClient createMQTTPublisher(String brokerAddr) throws MqttException {
        return new MqttClient(brokerAddr, UUID.randomUUID().toString());
    }
}
