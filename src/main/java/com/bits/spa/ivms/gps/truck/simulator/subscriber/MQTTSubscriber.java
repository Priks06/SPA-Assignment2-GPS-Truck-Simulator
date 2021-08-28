package com.bits.spa.ivms.gps.truck.simulator.subscriber;

import com.bits.spa.ivms.gps.truck.simulator.service.TruckData;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.List;
import java.util.UUID;

@Component
public class MQTTSubscriber {

    private static final Logger logger = LoggerFactory.getLogger(MQTTSubscriber.class);

    private IMqttClient subscriber;

    @Async("streamEvenHandlerTaskExecutor")
    public void listenToMessages(String brokerAddr, String topic) throws MqttException {
        subscriber = createMQTTPublisher(brokerAddr);
        connectClientToBroker();

        subscriber.subscribe(topic, (truckTopic, msg) -> {
            List<TruckData> truckDataList = interpretMessagesFromPayload(msg.getPayload());
            truckDataList.forEach(truckData -> logger.info("Data received: {}", truckData));
        });
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
    }
}
