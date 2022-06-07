package com.ankit.kafka.producers;

import com.ankit.kstreams.Address;
import com.ankit.kstreams.Employee;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class AddressDataProducer {

    @Autowired
    private KafkaTemplate<String, Address> kafkaTemplate;

    @Value("${address.input.topic.name}")
    private String addressTopic;

    private Logger logger = LoggerFactory.getLogger(AddressDataProducer.class);

    public void produce(Integer id, String country){
        Address address = new Address(id, country);
        kafkaTemplate.send(addressTopic, String.valueOf(id), address)
                .addCallback(
                        (successResult) -> logger.info("Successfully produced Address {} with key {}}", successResult.getProducerRecord().value(),
                                                        successResult.getProducerRecord().key())
                        ,(failureResult) -> logger.info("Unable to produce Address. Message - ", failureResult.getMessage())
                );
    }

}
