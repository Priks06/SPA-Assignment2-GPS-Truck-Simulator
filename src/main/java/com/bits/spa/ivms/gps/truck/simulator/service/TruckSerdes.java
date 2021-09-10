package com.bits.spa.ivms.gps.truck.simulator.service;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public final class TruckSerdes {

    static public final class TruckDataSerde extends Serdes.WrapperSerde<TruckData> {
        public TruckDataSerde() {
            super(new TruckDataSerializer(), new TruckDataDeserializer());
        }
    }

    public static Serde<TruckData> TruckData() {
        return new TruckDataSerde();
    }

    static public final class TruckSpeedSerde extends Serdes.WrapperSerde<TruckSpeed> {
        public TruckSpeedSerde() {
            super(new TruckSpeedSerializer(), new TruckSpeedDeserializer());
        }
    }

    public static Serde<TruckSpeed> TruckSpeed() {
        return new TruckSpeedSerde();
    }


}
