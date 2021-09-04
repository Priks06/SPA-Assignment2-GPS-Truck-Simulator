package com.bits.spa.ivms.gps.truck.simulator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

public class TruckDataDeserializer implements Deserializer<TruckData> {

    @Override
    public TruckData deserialize(String s, byte[] bytes) {
        ObjectMapper mapper = new ObjectMapper();
        TruckData user = null;
        try {
            user = mapper.readValue(bytes, TruckData.class);
        } catch (Exception e) {

            e.printStackTrace();
        }
        return user;
    }

    @Override
    public void close() {

    }
}
