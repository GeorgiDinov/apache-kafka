package com.georgidinov.libraryeventsconsumer.config;

import com.georgidinov.libraryeventsconsumer.entity.FailedRecordStatus;
import com.georgidinov.libraryeventsconsumer.service.FailureService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

@Slf4j
@EnableKafka
@Configuration
public class LibraryEventsConsumerConfig {

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;
    @Autowired
    FailureService failureService;

    @Value("${spring.kafka.topic.retry}")
    private String retryTopic;
    @Value("${spring.kafka.topic.dlt}")
    private String dltTopic;


    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory,
            ObjectProvider<ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>>> kafkaContainerCustomizer) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory.getIfAvailable());
        factory.setConcurrency(3);
        factory.setCommonErrorHandler(errorHandler());
        kafkaContainerCustomizer.ifAvailable(factory::setContainerCustomizer);
        return factory;
    }

    public DefaultErrorHandler errorHandler() {
        List<Class<? extends Exception>> exceptionsToIgnoreList = List.of(IllegalArgumentException.class);

        //List<Class<? extends Exception>> exceptionsToRetryList = List.of(RecoverableDataAccessException.class);

        FixedBackOff fixedBackOff = new FixedBackOff(1000L, 2);

        ExponentialBackOffWithMaxRetries exponentialBackOffWithMaxRetries = new ExponentialBackOffWithMaxRetries(2);
        exponentialBackOffWithMaxRetries.setInitialInterval(1_000L);
        exponentialBackOffWithMaxRetries.setMultiplier(2.0d);
        exponentialBackOffWithMaxRetries.setMaxInterval(2_000L);

        DefaultErrorHandler defaultErrorHandler = new DefaultErrorHandler(consumerRecordRecoverer, exponentialBackOffWithMaxRetries);
        defaultErrorHandler.setRetryListeners(((record, ex, deliveryAttempt) -> {
            log.info("Failed record in retry listener, Exception: {}, delivery attempt={}", ex.getMessage(), deliveryAttempt);
        }));

        exceptionsToIgnoreList.forEach(defaultErrorHandler::addNotRetryableExceptions);
        //exceptionsToRetryList.forEach(defaultErrorHandler::addRetryableExceptions);

        return defaultErrorHandler;
    }

    public DeadLetterPublishingRecoverer publishingRecoverer() {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate, (consumerRecord, exception) -> {
            if (exception.getCause() instanceof RecoverableDataAccessException) {
                return new TopicPartition(retryTopic, consumerRecord.partition());
            } else {
                return new TopicPartition(dltTopic, consumerRecord.partition());
            }
        });
        return recoverer;
    }

    public ConsumerRecordRecoverer consumerRecordRecoverer = ((consumerRecord, exception) -> {
        ConsumerRecord<Integer,String> record = (ConsumerRecord<Integer, String>) consumerRecord;
        if (exception.getCause() instanceof RecoverableDataAccessException) {
         // recovery logic
            log.info("Inside recovery logic");
            failureService.saveFailedRecord(record,exception, FailedRecordStatus.RETRY);
        } else {
         // non recovery logic
            log.info("Inside non recovery logic");
            failureService.saveFailedRecord(record,exception, FailedRecordStatus.DEAD);
        }
    });

}
