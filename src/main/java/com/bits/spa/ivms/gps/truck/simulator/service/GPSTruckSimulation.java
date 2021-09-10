package com.bits.spa.ivms.gps.truck.simulator.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Component
public class GPSTruckSimulation {

    private static final Logger logger = LoggerFactory.getLogger(GPSTruckSimulation.class);

    public List<TruckData> simulateTruckData(int driverId, String routeName) {
        List<TruckData> truckDataList = new ArrayList<>();
        TruckData prevTruckData;

        double currLatitude = getInitialLatitude();
        double currLongitude = getInitialLongitude();
        LocalDateTime dateTime = LocalDateTime.now();
        String currTimestamp = dateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);

        TruckData initialTruckData = formMockTruckData(driverId, routeName, currTimestamp, currLatitude, currLongitude, null);
        truckDataList.add(initialTruckData);
        prevTruckData = initialTruckData;


        DecimalFormat df = new DecimalFormat("#.#######");
//        logger.info("latitude:longitude --> " + df.format(currLatitude) + "," + df.format(currLongitude));
        for (int i = 0; i < 20; i++) {
            currLatitude = generateNextLatitudeData(currLatitude);
            currLongitude = generateNextLongitudeData(currLongitude);
//            logger.info("latitude:longitude --> " + df.format(currLatitude) + "," + df.format(currLongitude));
            dateTime = dateTime.plusSeconds(1);
            currTimestamp = dateTime.format((DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            TruckData truckData = formMockTruckData(driverId, routeName, currTimestamp, currLatitude, currLongitude, prevTruckData);
            truckDataList.add(truckData);
            prevTruckData = truckData;

        }
        return truckDataList;
    }

    private TruckData formMockTruckData(int driverId, String routeName, String timestamp, double latitude, double longitude, TruckData prevTruckData) {
        TruckData truckData = new TruckData();
        truckData.setDriverId(driverId);
        truckData.setRouteName(routeName);
        truckData.setCurrTimestamp(timestamp);
        truckData.setCurrLatitude(String.valueOf(latitude));
        truckData.setCurrLongitude(String.valueOf(longitude));
        if (!Objects.isNull(prevTruckData)) {
            truckData.setPrevTimestamp(prevTruckData.getCurrTimestamp());
            truckData.setPrevLatitude(prevTruckData.getCurrLatitude());
            truckData.setPrevLongitude(prevTruckData.getCurrLongitude());
        }
//        logger.info("Truck data: {}", truckData);
        return truckData;
    }

    private double getInitialLongitude() {
        double minLon = 73.00;
        double maxLon = 73.00;
        return minLon + (Math.random() * ((maxLon - minLon) + 1));
    }

    private double getInitialLatitude() {
        double minLat = 18.00;
        double maxLat = 19.00;
        return minLat + (Math.random() * ((maxLat - minLat) + 1));
    }

    private double generateNextLatitudeData(double currLatitude) {
        return currLatitude + 0.00004;
    }

    private double generateNextLongitudeData(double currLongitude) {
        return currLongitude + 0.00004;
    }

    public static void main(String[] args) {
        GPSTruckSimulation gpsTruckSimulator = new GPSTruckSimulation();
        List<TruckData> truckDataList = gpsTruckSimulator.simulateTruckData(11, "Trial Route");
        truckDataList.forEach(truckData -> logger.info("Mock data: {}", truckData));
    }
}
