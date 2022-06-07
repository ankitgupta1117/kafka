package com.ankit.kafka.config;

import com.ankit.kafka.processors.EmployeeEnricher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.stereotype.Component;

@Configuration
@EnableKafkaStreams
@ConditionalOnProperty(value = "enable.streams",havingValue = "true", matchIfMissing = true)
public class KafkaStreamsConfig {

    @Bean
    public EmployeeEnricher employeeEnricher(){
        return new EmployeeEnricher();
    }

}

