package com.ankit.kafka.processors;

import com.ankit.kstreams.Address;
import com.ankit.kstreams.Employee;
import com.ankit.kstreams.EnrichedEmployee;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.util.Map;

public class AddressStreamProcessor {

    public static final String SCHEMA_REGISTRY_URL_KEY = "schema.registry.url";

    @Value("${address.input.topic.name}")
    private String addressTopic;
    @Value("${schema.registry.url}")
    private String schemaRegistryUrl;

    @Autowired
    public void enrich(StreamsBuilder builder){
        // Serdes
        SpecificAvroSerde<Address> addressSerde = getAddressSpecificAvroSerde();
        Serde<String> stringSerde = Serdes.String();

        KTable<String, Address> addressKTable = builder.table(addressTopic
                                                    , Consumed.with(stringSerde, addressSerde)
                                                            .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST)
                                                    , Materialized.as("addressStore"));
    }

    private SpecificAvroSerde<Address> getAddressSpecificAvroSerde() {
        SpecificAvroSerde<Address> addressSerde = new SpecificAvroSerde<>();
        addressSerde.configure(Map.of(SCHEMA_REGISTRY_URL_KEY,schemaRegistryUrl),false);
        return addressSerde;
    }
}
