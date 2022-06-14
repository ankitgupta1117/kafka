package com.ankit.kafka.store.lookup.processors;

import com.ankit.kstreams.Country;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.util.Map;

public class CountryStreamProcessor {

    public static final String SCHEMA_REGISTRY_URL_KEY = "schema.registry.url";

    @Value("${country.input.topic.name}")
    private String countryTopic;
    @Value("${schema.registry.url}")
    private String schemaRegistryUrl;

    @Autowired
    public void process(StreamsBuilder builder){
        // Serdes
        SpecificAvroSerde<Country> countrySerde = getSerdeFor(Country.class);
        Serde<String> stringSerde = Serdes.String();
        builder.table(countryTopic, Consumed.with(stringSerde, countrySerde)
                                    .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST)
                                , Materialized.as("country-store"));
    }

    private <E extends SpecificRecord> SpecificAvroSerde<E> getSerdeFor(Class<E> sp) {
        SpecificAvroSerde<E> employeeSerde = new SpecificAvroSerde<>();
        employeeSerde.configure(Map.of(SCHEMA_REGISTRY_URL_KEY, schemaRegistryUrl),false);
        return employeeSerde;
    }

}
