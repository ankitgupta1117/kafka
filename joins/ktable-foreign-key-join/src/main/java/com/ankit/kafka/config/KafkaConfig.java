package com.ankit.kafka.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
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
    public KafkaAdmin.NewTopics topics(){
        NewTopic addressTopic = new NewTopic("address", 3, (short) 1);
        NewTopic employeeTopic = new NewTopic("employee", 3, (short) 1);
        NewTopic enrichedEmpTopic = new NewTopic("enriched-employee", 3, (short) 1);
        return new KafkaAdmin.NewTopics(addressTopic, employeeTopic, enrichedEmpTopic);
    }
}
