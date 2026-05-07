package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Sends a small batch of string records to {@code demo-topic} using the official Kafka client.
 */
public final class SimpleProducer {

    public static final String TOPIC = "demo-topic";

    public static void main(String[] args) {
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        int count = parseCountOrDefault(args, 10);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                final String key = "key-" + i;
                final String value = "plain-java-msg-" + i + " @ " + System.currentTimeMillis();
                producer.send(new ProducerRecord<>(TOPIC, key, value), (meta, err) -> {
                    if (err != null) {
                        err.printStackTrace();
                    } else {
                        System.out.printf("Published topic=%s partition=%d offset=%d key=%s value=%s%n",
                                meta.topic(), meta.partition(), meta.offset(), key, value);
                    }
                });
            }
            producer.flush();
        }
        System.out.println("Producer finished.");
    }

    private static int parseCountOrDefault(String[] args, int defaultCount) {
        if (args.length == 0) {
            return defaultCount;
        }
        try {
            return Math.max(1, Integer.parseInt(args[0]));
        } catch (NumberFormatException e) {
            return defaultCount;
        }
    }
}
