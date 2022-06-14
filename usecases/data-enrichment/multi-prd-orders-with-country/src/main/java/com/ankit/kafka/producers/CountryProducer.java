package com.ankit.kafka.producers;

import com.ankit.kstreams.Country;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class CountryProducer {

    @Autowired
    private KafkaTemplate<String, Country> kafkaTemplate;

    @Value("${country.input.topic.name}")
    private String countryTopic;

    private Logger logger = LoggerFactory.getLogger(CountryProducer.class);

    public void produce(Integer id, String countryName){
        Country country = new Country(id, countryName);
        kafkaTemplate.send(countryTopic, String.valueOf(id), country)
                .addCallback(
                        (successResult) -> logger.info("Successfully produced Country {} with key {}}", successResult.getProducerRecord().value(),
                                                        successResult.getProducerRecord().key())
                        ,(failureResult) -> logger.info("Unable to produce Country. Message - ", failureResult.getMessage())
                );
    }

}
