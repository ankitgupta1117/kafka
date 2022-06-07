package com.ankit.kafka.util;

import com.ankit.kafka.producers.EmployeeDataProducer;
import com.github.javafaker.Faker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
//@ConditionalOnProperty(value = "enable.employee.sample.data",havingValue = "true")
@ConditionalOnProperty(value = "enable.streams",havingValue = "false", matchIfMissing = false)
public class SampleEmployeeDataGenerator {

    @Autowired
    private EmployeeDataProducer empDataProducer;
    private Faker faker = new Faker();
    private Logger logger = LoggerFactory.getLogger(SampleEmployeeDataGenerator.class);

    @Scheduled(fixedDelay = 500l)
    public void generateEmployeeData(){

        int id = faker.number().numberBetween(1, 9999999);
        String name = faker.name().firstName();
        String status = "ENABLED";
        int address = faker.number().numberBetween(1,12);
//        logger.info("Producing Employee with ID: {}",id);
        empDataProducer.produce(id, name, status, address);
    }


}
