package com.bits.spa.ivms.gps.truck.simulator.mongodb;

import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.io.Serializable;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
@Document(collection = "truck_data")
public class TruckDataEntity implements Serializable {

    private static final long serialVersionUID = 3134117589354720891L;

    @Id
    private String recordId;

    @Indexed
    private int driverId;

    @Indexed
    private String routeName;

    private String timestamp;

    private String latitude;

    private String longitude;

}
