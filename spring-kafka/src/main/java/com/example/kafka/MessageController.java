package com.example.kafka;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api")
public class MessageController {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public MessageController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Body: {@code { "key": "optional-key", "message": "hello" }}
     */
    @PostMapping("/messages")
    public Map<String, Object> publish(@RequestBody PublishRequest body) {
        String key = body.key() != null ? body.key() : "spring";
        String message = body.message() != null ? body.message() : "";
        String payload = message + " @ " + System.currentTimeMillis();
        kafkaTemplate.send(SpringKafkaDemoApplication.TOPIC, key, payload)
                .whenComplete((r, ex) -> {
                    if (ex != null) {
                        ex.printStackTrace();
                    } else {
                        System.out.printf("[spring-producer] topic=%s partition=%d offset=%d key=%s value=%s%n",
                                r.getRecordMetadata().topic(),
                                r.getRecordMetadata().partition(),
                                r.getRecordMetadata().offset(),
                                key,
                                payload);
                    }
                });
        return Map.of(
                "topic", SpringKafkaDemoApplication.TOPIC,
                "key", key,
                "payload", payload,
                "status", "accepted");
    }

    public record PublishRequest(String key, String message) {
    }
}
