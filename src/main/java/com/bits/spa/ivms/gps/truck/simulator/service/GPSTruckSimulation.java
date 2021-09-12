package com.bits.spa.ivms.gps.truck.simulator.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Component
public class GPSTruckSimulation {

    private static final Logger logger = LoggerFactory.getLogger(GPSTruckSimulation.class);

    public List<TruckData> simulateTruckData(int driverId, String routeName, int geoFileIndex) {
        List<TruckData> truckDataList;
        TruckData prevTruckData = null;
        LocalDateTime dateTime = LocalDateTime.now(ZoneId.of("UTC"));
        String currTimestamp;


        truckDataList = Optional.of(GeoJSONHelper.readGeoJsonFile(geoFileIndex)).orElse(new ArrayList<>());
        for (TruckData truckData : truckDataList) {
            truckData.setDriverId(driverId);
            truckData.setRouteName(routeName);
            dateTime = dateTime.plusSeconds(5);
            currTimestamp = dateTime.format((DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            truckData.setCurrTimestamp(currTimestamp);
            if (Objects.nonNull(prevTruckData)) {
                truckData.setPrevTimestamp(prevTruckData.getCurrTimestamp());
                truckData.setPrevLatitude(prevTruckData.getCurrLatitude());
                truckData.setPrevLongitude(prevTruckData.getCurrLongitude());
            }
            prevTruckData = truckData;
        }

        return truckDataList;
    }

    public static void main(String[] args) {
        GPSTruckSimulation gpsTruckSimulator = new GPSTruckSimulation();
        List<TruckData> truckDataList = gpsTruckSimulator.simulateTruckData(11, "Trial Route", 1);
        truckDataList.forEach(truckData -> logger.info("Mock data: {}", truckData));
    }
}
