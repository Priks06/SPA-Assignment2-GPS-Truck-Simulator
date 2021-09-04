package com.bits.spa.ivms.gps.truck.simulator.kafka.consumer;

public class DistanceCalculatorService {

    static double haversine(double lat1, double lon1,
                            double lat2, double lon2)
    {
        // distance between latitudes and longitudes
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);

        // convert to radians
        lat1 = Math.toRadians(lat1);
        lat2 = Math.toRadians(lat2);

        // apply formulae
        double a = Math.pow(Math.sin(dLat / 2), 2) +
                Math.pow(Math.sin(dLon / 2), 2) *
                        Math.cos(lat1) *
                        Math.cos(lat2);
        double rad = 6371;
        double c = 2 * Math.asin(Math.sqrt(a));
        return rad * c;
    }

    // Driver Code
    public static void main(String[] args)
    {
        double lat1 = 18.59951102768258;
        double lon1 = 73.65415441355053;
        double lat2 = 18.59955102768258;
        double lon2 = 73.65419441355053;
        System.out.println(haversine(lat1, lon1, lat2, lon2) + " K.M.");
    }

}
