package com.ankit.kafka.config;

import com.ankit.kafka.processors.OrdersEnricher;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Configuration
@EnableKafkaStreams
@ConditionalOnProperty(value = "enable.streams",havingValue = "true", matchIfMissing = false)
public class KafkaStreamsConfig {

    @Bean
    public OrdersEnricher ordersEnricher(){
        return new OrdersEnricher();
    }

}

