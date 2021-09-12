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
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Component
public class MQTTPublisher {

    private static final List<String> routeNames = Arrays.asList("Mumbai Expressway", "Agra Expressway", "Golden Quadrilateral");

    private static final List<Integer> driverIds = Arrays.asList(100, 101, 102, 103, 104);

    private Random random = new Random();

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
            for (int i = 0; i < driverIds.size(); i++) {
                String routeName = routeNames.get(random.nextInt(routeNames.size()));
                int driverId = driverIds.get(i);
                MqttMessage mockTruckData = getTruckDataPayload(routeName, driverId, i+1);
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
