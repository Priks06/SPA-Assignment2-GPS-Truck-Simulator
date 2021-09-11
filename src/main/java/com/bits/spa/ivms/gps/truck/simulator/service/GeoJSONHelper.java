package com.bits.spa.ivms.gps.truck.simulator.service;

import org.bson.BsonArray;
import org.bson.BsonValue;
import org.bson.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class GeoJSONHelper {

    private static final Logger logger = LoggerFactory.getLogger(GeoJSONHelper.class);

    public static List<TruckData> readGeoJsonFile(int geoFileIndex) {
        List<TruckData> truckDataList = new ArrayList<>();
        try {
            String resourceLocation = "classpath:geo-files/map-" + geoFileIndex + ".geojson";
            File file = ResourceUtils.getFile(resourceLocation);
            InputStream inputStream = new FileInputStream(file);
            byte[] byteData = FileCopyUtils.copyToByteArray(inputStream);
            String data = new String(byteData, StandardCharsets.UTF_8);
            logger.info(data);

            JsonObject jsonObject = new JsonObject(data);
            BsonArray features = jsonObject.toBsonDocument().get("features").asArray();
            BsonValue value = features.get(0).asDocument().get("geometry").asDocument().get("coordinates").asArray().get(0).asArray().get(0);
            logger.info("Trying some bson shit: {}", value);

            features.get(0).asDocument().get("geometry").asDocument().get("coordinates").asArray().forEach(bsonValue -> {
                double longitude = bsonValue.asArray().get(0).asDouble().doubleValue();
                double latitude = bsonValue.asArray().get(1).asDouble().doubleValue();
                logger.info("Geo Points: latitude: {} longitude: {}", latitude, longitude);
                TruckData truckData = new TruckData();
                truckData.setCurrLatitude(String.valueOf(latitude));
                truckData.setCurrLongitude(String.valueOf(longitude));
                truckDataList.add(truckData);
            });

        } catch (Exception e) {
            logger.error("Something went wrong while reading GeoJSON file. ", e);
        }
        return truckDataList;
    }

}
