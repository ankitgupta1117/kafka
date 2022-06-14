package com.ankit.kafka.store.lookup.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public KafkaAdmin kafkaAdmin(){
        Map<String, Object> config = Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new KafkaAdmin(config);
    }

    @Bean
    public KafkaAdmin.NewTopics topics(@Value("${orders.input.topic.name}") String ordersTopicName
                                        , @Value("${country.input.topic.name}") String countryTopicName
                                        , @Value("${output.topic.name}") String outputTopicName){
        NewTopic ordersTopic = new NewTopic(ordersTopicName, 3, (short) 1);
        NewTopic countryTopic = new NewTopic(countryTopicName, 3, (short) 1);
        NewTopic enrichedOrders = new NewTopic(outputTopicName, 3, (short) 1);
        return new KafkaAdmin.NewTopics(ordersTopic, countryTopic, enrichedOrders);
    }
}
