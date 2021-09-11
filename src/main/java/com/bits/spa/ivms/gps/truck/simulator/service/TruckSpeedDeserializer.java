package com.bits.spa.ivms.gps.truck.simulator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

public class TruckSpeedDeserializer implements Deserializer<TruckSpeed> {

    @Override
    public TruckSpeed deserialize(String s, byte[] bytes) {
        ObjectMapper mapper = new ObjectMapper();
        TruckSpeed truckSpeed = null;
        try {
            truckSpeed = mapper.readValue(bytes, TruckSpeed.class);
        } catch (Exception e) {

            e.printStackTrace();
        }
        return truckSpeed;
    }

    @Override
    public void close() {

    }
}
