package com.bits.spa.ivms.gps.truck.simulator.service;

import lombok.*;

import java.io.Serializable;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class TruckSpeed implements Serializable {

    private static final long serialVersionUID = 3134117589354720891L;

    private int driverId;

    private String routeName;

    private String timestamp;

    private String speed;

}
