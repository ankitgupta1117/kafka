package com.ankit.kafka.store.lookup.util;

import com.ankit.kafka.store.lookup.producers.CountryProducer;
import com.github.javafaker.Faker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
@ConditionalOnExpression(value = " ${enable.countries.sample.data:false} " +
                                    " && !${enable.orders.sample.data:false}" +
                                    " && !${enable.streams:false}")
public class SampleCountryDataGenerator {

    @Autowired
    private CountryProducer countryProducer;
    private Faker faker = new Faker();
    private Logger logger = LoggerFactory.getLogger(SampleCountryDataGenerator.class);

    @PostConstruct
    public void generateCountryData() {
        Faker faker = new Faker();
        int i = 1;
        while (i < 13) {
            String country = faker.country().name();
            countryProducer.produce(i, country);
            i++;
        }
    }

}

