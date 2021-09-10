package com.bits.spa.ivms.gps.truck.simulator.service;

import lombok.*;

import java.io.Serializable;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class TruckData implements Serializable {

    private static final long serialVersionUID = 3134117589354720891L;

    private int driverId;

    private String routeName;

    private String currTimestamp;
    private String currLatitude;
    private String currLongitude;

    private String prevTimestamp;
    private String prevLatitude;
    private String prevLongitude;

}
