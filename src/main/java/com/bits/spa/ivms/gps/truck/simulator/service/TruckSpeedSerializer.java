package com.bits.spa.ivms.gps.truck.simulator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class TruckSpeedSerializer implements Serializer<TruckSpeed> {

    @Override
    public byte[] serialize(String s, TruckSpeed truckSpeed) {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            retVal = objectMapper.writeValueAsString(truckSpeed).getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retVal;
    }

    @Override
    public void close() {

    }
}
