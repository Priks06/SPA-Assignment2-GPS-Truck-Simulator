package com.bits.spa.ivms.gps.truck.simulator.mongodb;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TruckDataRepository extends MongoRepository<TruckDataEntity, String> {
}
