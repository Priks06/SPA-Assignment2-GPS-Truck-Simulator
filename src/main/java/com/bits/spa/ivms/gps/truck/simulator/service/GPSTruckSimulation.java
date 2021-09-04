package com.bits.spa.ivms.gps.truck.simulator.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@Component
public class GPSTruckSimulation {

    private static final Logger logger = LoggerFactory.getLogger(GPSTruckSimulation.class);

    public List<TruckData> simulateTruckData(int driverId, String routeName) {
        List<TruckData> truckDataList = new ArrayList<>();

        double currLatitude = getInitialLatitude();
        double currLongitude = getInitialLongitude();
        LocalDateTime dateTime = LocalDateTime.now();
        String currTimestamp = dateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);

        truckDataList.add(formMockTruckData(driverId, routeName, currTimestamp, currLatitude, currLongitude));


        DecimalFormat df = new DecimalFormat("#.#######");
//        logger.info("latitude:longitude --> " + df.format(currLatitude) + "," + df.format(currLongitude));
        for (int i = 0; i < 20; i++) {
            currLatitude = generateNextLatitudeData(currLatitude);
            currLongitude = generateNextLongitudeData(currLongitude);
//            logger.info("latitude:longitude --> " + df.format(currLatitude) + "," + df.format(currLongitude));
            dateTime = dateTime.plusSeconds(1);
            currTimestamp = dateTime.format((DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            truckDataList.add(formMockTruckData(driverId, routeName, currTimestamp, currLatitude, currLongitude));

        }
        return truckDataList;
    }

    private TruckData formMockTruckData(int driverId, String routeName, String timestamp, double latitude, double longitude) {
        TruckData truckData = new TruckData(driverId, routeName, timestamp, String.valueOf(latitude), String.valueOf(longitude));
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
}
