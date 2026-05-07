package com.example.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class DemoTopicListener {

    @KafkaListener(
            topics = SpringKafkaDemoApplication.TOPIC,
            groupId = "${spring.kafka.consumer.group-id}")
    public void onMessage(String payload) {
        System.out.printf("[spring-consumer] value=%s%n", payload);
    }
}
