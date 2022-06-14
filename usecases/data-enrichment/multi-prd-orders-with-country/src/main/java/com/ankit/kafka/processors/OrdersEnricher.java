package com.ankit.kafka.processors;

import com.ankit.kstreams.Country;
import com.ankit.kstreams.Order;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OrdersEnricher {

    private static final Logger logger = LoggerFactory.getLogger(OrdersEnricher.class);
    public static final String SCHEMA_REGISTRY_URL_KEY = "schema.registry.url";

    @Value("${orders.input.topic.name}")
    private String ordersTopic;
    @Value("${country.input.topic.name}")
    private String countryTopic;
    @Value("${output.topic.name}")
    private String outputTopic;
    @Value("${schema.registry.url}")
    private String schemaRegistryUrl;

    @Autowired
    public void enrich(StreamsBuilder builder){
        // Serdes
        SpecificAvroSerde<Order> orderSerde = getSerdeFor(Order.class);
        SpecificAvroSerde<Country> countrySerde = getSerdeFor(Country.class);
        Serde<String> stringSerde = Serdes.String();
        KTable<String, Country> countryKTable = builder.table(countryTopic, Consumed.with(stringSerde, countrySerde)
                                                                                    .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST));
        KStream<String, Order> orderKStream = builder.stream(ordersTopic, Consumed.with(stringSerde, orderSerde)
                                                                                    .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST))
                                                            .peek((k, v) -> logger.info("Observed event: Order ID: {}, Products: {}", k, v.getProducts()));
        orderKStream
                .flatMap((String key, Order order) -> {
                    List<KeyValue<String, Order>> keyValues = order.getProducts().stream()
                            .map(product -> {
                                Order o = new Order();
                                o.setId(order.getId());
                                o.setStatus(order.getStatus());
                                o.setProducts(List.of(product));
                                KeyValue<String, Order> keyValue = new KeyValue(String.valueOf(product.getCountryCode()), o);
                                return keyValue;
                            })
                            .collect(Collectors.toList());
                    return keyValues;
                })
                .join(countryKTable
                        , (Order order, Country country) -> {
                            String countryName = country.getCountry();
                            order.getProducts().get(0).setCountry(countryName);
                            return order;
                        }
                        , Joined.with(stringSerde, orderSerde, countrySerde))
                .selectKey((countryCode, order) -> String.valueOf(order.getId()))
                .groupByKey(Grouped.with(stringSerde, orderSerde))
                .aggregate(Order::new, (orderId, order, finalOrder) -> {
                    finalOrder.setId(order.getId());
                    finalOrder.setStatus(order.getStatus());
                    if(null == finalOrder.getProducts())
                        finalOrder.setProducts(order.getProducts());
                    else
                        finalOrder.getProducts().addAll(order.getProducts());
                    return finalOrder;
                }, Materialized.with(stringSerde, orderSerde))
                .toStream()
                .peek( (k, v) -> logger.info("Enriched Order: ID: {}, Products: {}", k, v.getProducts()))
                .to(outputTopic);
    }


    private <E extends SpecificRecord> SpecificAvroSerde<E> getSerdeFor(Class<E> sp) {
        SpecificAvroSerde<E> employeeSerde = new SpecificAvroSerde<>();
        employeeSerde.configure(Map.of(SCHEMA_REGISTRY_URL_KEY, schemaRegistryUrl),false);
        return employeeSerde;
    }
}