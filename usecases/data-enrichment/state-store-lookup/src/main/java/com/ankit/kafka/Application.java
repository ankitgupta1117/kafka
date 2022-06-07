package com.ankit.kafka;

import com.ankit.kafka.producers.AddressDataProducer;
import com.github.javafaker.Faker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class Application {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        ApplicationContext context = SpringApplication.run(Application.class, args);
        AddressDataProducer addressDataProducer = context.getBean(AddressDataProducer.class);
//        generateAddresData(addressDataProducer);
    }

    private static void generateAddresData(AddressDataProducer addressDataProducer) {
        Faker faker = new Faker();
        int i = 1;
        while (i < 12) {
            try {
                String country = faker.country().name();
                addressDataProducer.produce(i, country);
                i++;
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.error(e.toString());
            }
        }
    }
}