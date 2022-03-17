package com.github.littlewoo.kafka;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.OffsetStrategy;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.event.ShutdownEvent;
import io.micronaut.messaging.annotation.MessageBody;
import io.micronaut.runtime.event.annotation.EventListener;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@KafkaListener(
    groupId = "MyConsumer",
    offsetReset = OffsetReset.EARLIEST,
    offsetStrategy = OffsetStrategy.SYNC_PER_RECORD
)
public class MyConsumer {
    int count = 0;

    private static final Logger log = LoggerFactory.getLogger(MyConsumer.class);

    public ConcurrentHashMap<String, String> getMessages() {
        return messages;
    }

    private final ConcurrentHashMap<String, String> messages = new ConcurrentHashMap<>();

    @Topic("my-topic")
    public void consumeRaw(ConsumerRecord<String, String> record) {
        var message = record.value();
        var offset = record.offset();
        var partition = record.partition();

        count++;
        if (count < 501) {
            throw new RuntimeException("I can't let you do that");
        }
        messages.put(String.format("%s,%s", partition, offset), message);
        log.info("Reading message {} on partition={}", message, partition);
    }

    @EventListener
    public void onShutdownEvent(ShutdownEvent event) {
        log.info("########### SHUTDOWN ##########");
        log.info("nr messages: {}", messages.size());
        log.info(messages.toString());
        log.info("###############################");
    }
}
