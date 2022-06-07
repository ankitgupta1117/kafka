package com.ankit.kafka.processors;

import com.ankit.kstreams.Address;
import com.ankit.kstreams.Employee;
import com.ankit.kstreams.EnrichedEmployee;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Properties;

public class EmployeeEnricher {

    private static final Logger logger = LoggerFactory.getLogger(EmployeeEnricher.class);
    public static final String SCHEMA_REGISTRY_URL_KEY = "schema.registry.url";

    @Value("${address.input.topic.name}")
    private String addressTopic;
    @Value("${employee.input.topic.name}")
    private String employeeTopic;
    @Value("${output.topic.name}")
    private String outputTopic;
    @Value("${schema.registry.url}")
    private String schemaRegistryUrl;

    @Autowired
    public void enrich(StreamsBuilder builder){
        // Serdes
        SpecificAvroSerde<Address> addressSerde = getAddressSpecificAvroSerde();
        SpecificAvroSerde<Employee> employeeSerde = getEmployeeSpecificAvroSerde();
        SpecificAvroSerde<EnrichedEmployee> enrichedEmployeeSerde = getEnrichedEmployeeSpecificAvroSerde();
        Serde<String> stringSerde = Serdes.String();
        KTable<String, Address> addressKTable = builder.table(addressTopic, Consumed.with(stringSerde, addressSerde));
        builder.stream(employeeTopic, Consumed.with(stringSerde, employeeSerde))
                .peek((k, v) -> logger.info("Observed event: ID: {}, Address: {}", v.getId(), v.getAddress()))
                .toTable()
                .join(addressKTable
                        , employee -> employee.getAddress()
                        ,(employee, address) -> new EnrichedEmployee(employee.getId(), employee.getName(), employee.getStatus(), address.getCountry())
                )
                .toStream()
                .peek((k,v) -> logger.info("Enriched event: ID: {}, Address: {}", v.getId(), v.getCountry()))
                .to(outputTopic, Produced.with(stringSerde, enrichedEmployeeSerde));
    }

    public Topology process(String countryTopic, String employeeTopic, String outputTopic) {
        // Serdes
        SpecificAvroSerde<Address> addressSerde = getAddressSpecificAvroSerde();
        SpecificAvroSerde<Employee> employeeSerde = getEmployeeSpecificAvroSerde();
        SpecificAvroSerde<EnrichedEmployee> encrichedEmployeeSerde = getEnrichedEmployeeSpecificAvroSerde();
        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, Address> addressKTable = builder.table(countryTopic, Consumed.with(stringSerde, addressSerde));
        KStream<String, Employee> employeeKStream = builder.stream(employeeTopic, Consumed.with(stringSerde, employeeSerde));

        ValueJoiner<Employee, Address, EnrichedEmployee> joiner =  (employee, address) -> {
            return new EnrichedEmployee(employee.getId(), employee.getName(), employee.getStatus(),address.getCountry());
        };
        employeeKStream
                .peek((k,v) -> logger.info("Observed event: {}", v))
                .join(addressKTable,joiner)
                .peek((k,v) -> logger.info("Transformed event: {}", v))
                .to(outputTopic, Produced.with(stringSerde, encrichedEmployeeSerde));

        return builder.build();
    }

    private SpecificAvroSerde<EnrichedEmployee> getEnrichedEmployeeSpecificAvroSerde() {
        SpecificAvroSerde<EnrichedEmployee> encrichedEmployeeSerde = new SpecificAvroSerde<>();
        encrichedEmployeeSerde.configure(Map.of(SCHEMA_REGISTRY_URL_KEY,schemaRegistryUrl),false);
        return encrichedEmployeeSerde;
    }

    private SpecificAvroSerde<Address> getAddressSpecificAvroSerde() {
        SpecificAvroSerde<Address> addressSerde = new SpecificAvroSerde<>();
        addressSerde.configure(Map.of(SCHEMA_REGISTRY_URL_KEY,schemaRegistryUrl),false);
        return addressSerde;
    }

    private SpecificAvroSerde<Employee> getEmployeeSpecificAvroSerde() {
        SpecificAvroSerde<Employee> employeeSerde = new SpecificAvroSerde<>();
        employeeSerde.configure(Map.of(SCHEMA_REGISTRY_URL_KEY, schemaRegistryUrl),false);
        return employeeSerde;
    }

}