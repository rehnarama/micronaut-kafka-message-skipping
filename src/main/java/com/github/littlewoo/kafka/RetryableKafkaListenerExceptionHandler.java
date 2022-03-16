package com.github.littlewoo.kafka;

import io.micronaut.configuration.kafka.exceptions.DefaultKafkaListenerExceptionHandler;
import io.micronaut.configuration.kafka.exceptions.KafkaListenerException;
import io.micronaut.configuration.kafka.exceptions.KafkaListenerExceptionHandler;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.core.annotation.NonNull;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Replaces(DefaultKafkaListenerExceptionHandler.class)
@Singleton
public class RetryableKafkaListenerExceptionHandler implements KafkaListenerExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(MyConsumer.class);

    @Override
    public void handle(final KafkaListenerException exception) {
        final Throwable cause = exception.getCause();
        final Object consumerBean = exception.getKafkaListener();
        Optional<ConsumerRecord<?, ?>> consumerRecord = exception.getConsumerRecord();
        log.error("Error processing record [{}] for Kafka consumer [{}] produced error: [{}]",
            consumerRecord, consumerBean, cause
        );
        if (consumerRecord.isPresent()) {
            seekBackToFailedRecordOnException(consumerRecord.get(), exception.getKafkaConsumer());
        } else {
            log.error("Kafka consumer [{}] produced error: record not found for listener exception: [{}]",
                consumerBean, cause
            );
        }
    }

    protected void seekBackToFailedRecordOnException(
        @NonNull ConsumerRecord<?, ?> record,
        @NonNull Consumer<?, ?> kafkaConsumer
    ) {
        try {
            log.warn("Seeking back to failed consumer record for partition {}-{} and offset {}",
                record.topic(), record.partition(), record.offset()
            );
            kafkaConsumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset());
        } catch (IllegalArgumentException | IllegalStateException e) {
            log.error(
                "Kafka consumer failed to seek offset to processing exception record: [{}] with error: [{}]",
                record,
                e
            );
        }
    }
}
