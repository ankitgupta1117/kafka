package com.ankit.kafka.store.lookup.processors;

import com.ankit.kstreams.Order;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
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
    public void enrich(StreamsBuilder builder){
        // Serdes
        SpecificAvroSerde<Order> orderSerde = getSerdeFor(Order.class);
        Serde<String> stringSerde = Serdes.String();

        KStream<String, Order> orderKStream =
                builder.stream(ordersTopic, Consumed
                                                    .with(stringSerde, orderSerde)
                                                    .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST))
                        .peek((k, v) -> log.info("Observed event: Order ID: {}, Products: {}", k, v.getProducts()));
    }


    private <E extends SpecificRecord> SpecificAvroSerde<E> getSerdeFor(Class<E> sp) {
        SpecificAvroSerde<E> employeeSerde = new SpecificAvroSerde<>();
        employeeSerde.configure(Map.of(SCHEMA_REGISTRY_URL_KEY, schemaRegistryUrl),false);
        return employeeSerde;
    }
}
