package com.bits.spa.ivms.gps.truck.simulator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

public class TruckDataDeserializer implements Deserializer<TruckData> {

    @Override
    public TruckData deserialize(String s, byte[] bytes) {
        ObjectMapper mapper = new ObjectMapper();
        TruckData truckData = null;
        try {
            truckData = mapper.readValue(bytes, TruckData.class);
        } catch (Exception e) {

            e.printStackTrace();
        }
        return truckData;
    }

    @Override
    public void close() {

    }
}
