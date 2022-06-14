package com.ankit.kafka.store.lookup.util;

import com.ankit.kafka.store.lookup.producers.OrderProducer;
import com.github.javafaker.Faker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnExpression(value = "${enable.orders.sample.data:false} && !${enable.streams:false}")
public class SampleOrderDataGenerator {

    @Autowired
    private OrderProducer orderDataProducer;
    private Faker faker = new Faker();
    private Logger logger = LoggerFactory.getLogger(SampleOrderDataGenerator.class);

    @Scheduled(fixedDelay = 50000l)
    public void generateOrders(){

        int id = faker.number().numberBetween(1, 9999999);
        String status = "ENABLED";
        orderDataProducer.produce(id, status, faker.number().numberBetween(100, 9000), faker.number().numberBetween(100, 9000));
    }


}
