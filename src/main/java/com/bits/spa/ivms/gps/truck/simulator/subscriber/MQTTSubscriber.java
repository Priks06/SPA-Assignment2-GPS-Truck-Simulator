package com.bits.spa.ivms.gps.truck.simulator.subscriber;

import com.bits.spa.ivms.gps.truck.simulator.service.TruckData;
import com.bits.spa.ivms.gps.truck.simulator.service.TruckDataSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

@Component
public class MQTTSubscriber {

    private static final Logger logger = LoggerFactory.getLogger(MQTTSubscriber.class);

    @Value("${kafka.bootstrap.servers}")
    String bootstrapServer;

    @Value("${kafka.topic}")
    String kafkaTopic;

    private IMqttClient subscriber;

    private KafkaProducer<String, TruckData> kafkaProducer;

    @Async("streamEvenHandlerTaskExecutor")
    public void listenToMessages(String brokerAddr, String topic) throws MqttException {
        subscriber = createMQTTPublisher(brokerAddr);
        connectClientToBroker();
        createKafkaProducer();

        subscriber.subscribe(topic, (truckTopic, msg) -> {
            List<TruckData> truckDataList = interpretMessagesFromPayload(msg.getPayload());
            truckDataList.forEach(truckData -> {
                logger.info("Data received: {}", truckData);

                ProducerRecord<String, TruckData> record = new ProducerRecord<>(kafkaTopic, truckData);
                logger.info("Kafka producer record: {}", record.value());
                kafkaProducer.send(record, (recordMetadata, e) -> {
                    if (e == null) {
                        // Successfully sent
                        logger.info("Topic: {} __ Partition: {} __ Offset: {}", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
                    } else {
                        logger.error("Something went wrong while publishing data to Kafka. ", e);
                    }
                });
            });
        });
    }

    private void createKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, TruckDataSerializer.class.getName());

        kafkaProducer = new KafkaProducer<>(properties);
    }

    @SuppressWarnings("unchecked")
    private List<TruckData> interpretMessagesFromPayload(byte[] payload) {
        List<TruckData> truckDataList = null;
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(payload))) {
            truckDataList = (List<TruckData>) ois.readObject();
        } catch (Exception e) {
            logger.error("Something went wrong while reading payload, ", e);
        }
        return truckDataList;
    }

    private void connectClientToBroker() throws MqttException {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);
        options.setConnectionTimeout(20);
        subscriber.connect(options);
    }

    public IMqttClient createMQTTPublisher(String brokerAddr) throws MqttException {
        return new MqttClient(brokerAddr, UUID.randomUUID().toString());
    }

    @PreDestroy
    public void closeMQTTConnection() throws MqttException {
        subscriber.disconnect();
        subscriber.close();
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
