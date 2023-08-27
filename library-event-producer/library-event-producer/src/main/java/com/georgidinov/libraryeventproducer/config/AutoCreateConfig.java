package com.georgidinov.libraryeventproducer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class AutoCreateConfig {

    @Value("${spring.kafka.topic}")
    private String kafkaTopic;

    @Bean
    public NewTopic libraryEvents() {
        return TopicBuilder
                .name(kafkaTopic)
                .partitions(3)
                .replicas(3)
                .build();
    }

}