package com.ankit.kafka.store.lookup.producers;

import com.ankit.kstreams.Order;
import com.ankit.kstreams.Product;
import com.github.javafaker.Faker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class OrderProducer {

    @Autowired
    private KafkaTemplate<String, Order> kafkaTemplate;
    @Value("${orders.input.topic.name}")
    private String orderTopicName;
    private Faker faker = new Faker();

    private Logger logger = LoggerFactory.getLogger(OrderProducer.class);

    public void produce(Integer id, String status, int... productIds){

        List<Product> productList = Arrays.stream(productIds)
                                            .mapToObj(productId -> Product.newBuilder()
                                                                                .setProductId(productId)
                                                                                .setCountryCode(faker.number().numberBetween(1, 12))
                                                                                .setProductName("Product-" + productId)
                                                                                .build())
                                            .collect(Collectors.toList());
        Order order = new Order(id, status, productList);

        kafkaTemplate.send(orderTopicName, String.valueOf(id), order)
                    .addCallback(
                            (successResult) -> logger.info("Successfully produced Order {} with key {}", successResult.getProducerRecord().value(),
                                                successResult.getProducerRecord().key())
                            ,(failureResult) -> logger.info("Unable to produce Order. Message - ", failureResult.getMessage())
                    );
    }


}
