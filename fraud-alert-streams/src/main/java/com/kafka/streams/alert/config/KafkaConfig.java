package com.kafka.streams.alert.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaConfig {

    @Value("${app.kafka.topics.fraud-input:fraud-detected-transactions}")
    private String fraudInputTopic;

    @Value("${app.kafka.topics.alerts-output:alerts}")
    private String alertsOutputTopic;

    @Bean
    public NewTopic fraudInputTopic() {
        return TopicBuilder.name(fraudInputTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic alertsOutputTopic() {
        return TopicBuilder.name(alertsOutputTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
