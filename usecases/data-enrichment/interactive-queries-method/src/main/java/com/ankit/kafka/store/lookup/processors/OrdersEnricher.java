package com.ankit.kafka.store.lookup.processors;

import com.ankit.kstreams.Order;
import com.ankit.rest.services.CountryService;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.util.Map;

@Slf4j
public class OrdersEnricher {

    public static final String SCHEMA_REGISTRY_URL_KEY = "schema.registry.url";

    @Value("${orders.input.topic.name}")
    private String ordersTopic;
    @Value("${output.topic.name}")
    private String outputTopic;
    @Value("${schema.registry.url}")
    private String schemaRegistryUrl;
    @Autowired
    private CountryService countryService;

    @Autowired
    public void enrich(StreamsBuilder builder){
        // Serdes
        SpecificAvroSerde<Order> orderSerde = getSerdeFor(Order.class);
        Serde<String> stringSerde = Serdes.String();

        builder.stream(ordersTopic, Consumed
                                            .with(stringSerde, orderSerde)
                                            .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST))
                .peek((k, v) -> log.info("Observed event: Order ID: {}, Products: {}", k, v.getProducts()))
                .mapValues(enrichOrderWithCountry())
                .peek((k,v) -> log.info("Enriched Order: ID: {}, Products: {}", k, v.getProducts()));
//                .to(outputTopic);

    }


    private <E extends SpecificRecord> SpecificAvroSerde<E> getSerdeFor(Class<E> sp) {
        SpecificAvroSerde<E> employeeSerde = new SpecificAvroSerde<>();
        employeeSerde.configure(Map.of(SCHEMA_REGISTRY_URL_KEY, schemaRegistryUrl),false);
        return employeeSerde;
    }

    private ValueMapper<Order, Order> enrichOrderWithCountry(){
        ValueMapper<Order, Order> enricher = (Order order) -> {
            order.getProducts().stream()
                    .forEach(product -> {
                        String countryCode = String.valueOf(product.getCountryCode());
                        product.setCountry(countryService.getCountry(countryCode));
                    });
            return order;
        };
        return enricher;
    }
}
