package com.bits.spa.ivms.gps.truck.simulator.publisher;

import com.bits.spa.ivms.gps.truck.simulator.service.GPSTruckSimulation;
import com.bits.spa.ivms.gps.truck.simulator.service.TruckData;
import org.eclipse.paho.client.mqttv3.*;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Component
public class MQTTPublisher {

    private final GPSTruckSimulation gpsTruckSimulation;

    private IMqttClient publisher;

    public MQTTPublisher(GPSTruckSimulation gpsTruckSimulation) {
        this.gpsTruckSimulation = gpsTruckSimulation;
    }

    @Async("streamEvenHandlerTaskExecutor")
    public void publishMessages(String brokerAddr, String topic) throws InterruptedException, MqttException, IOException {
        publisher = createMQTTPublisher(brokerAddr);
        connectClientToBroker();

        if (publisher.isConnected()) {
            int randomId = (int) (Math.random() * 100);
            String routeName = "Route " + randomId;
            int driverId = (int) (Math.random() * 100);
            MqttMessage mockTruckData = getTruckDataPayload(routeName, driverId, 1);
            publishSingleTruckMessage(mockTruckData, topic);

            for (int i = 0; i < 1; i++) {
                routeName = "Route " + (++randomId);
                ++driverId;
                mockTruckData = getTruckDataPayload(routeName, driverId, i+2);
                publishSingleTruckMessage(mockTruckData, topic);
                TimeUnit.SECONDS.sleep(5);
            }
        }

    }

    private void publishSingleTruckMessage(MqttMessage message, String topic) throws MqttException {
        message.setQos(0);
        message.setRetained(true);
        publisher.publish(topic, message);
    }

    private MqttMessage getTruckDataPayload(String routeName, int driverId, int geoFileIndex) throws IOException {
        // TODO: Spawn async threads which will generate mock data for different drivers in parallel.
        List<TruckData> mockTruckData = gpsTruckSimulation.simulateTruckData(driverId, routeName, geoFileIndex);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(mockTruckData);
        return new MqttMessage(bos.toByteArray());
    }

    private void connectClientToBroker() throws MqttException {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);
        options.setConnectionTimeout(20);
        publisher.connect(options);
    }

    public IMqttClient createMQTTPublisher(String brokerAddr) throws MqttException {
        return new MqttClient(brokerAddr, UUID.randomUUID().toString());
    }

    @PreDestroy
    public void closeMQTTConnection() throws MqttException {
        publisher.disconnect();
        publisher.close();
    }
}
