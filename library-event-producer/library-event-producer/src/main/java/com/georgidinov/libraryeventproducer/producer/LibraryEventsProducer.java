package com.georgidinov.libraryeventproducer.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.georgidinov.libraryeventproducer.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Component
public class LibraryEventsProducer {

    @Value("${spring.kafka.topic}")
    private String kafkaTopic;

    private final KafkaTemplate<Integer, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public LibraryEventsProducer(KafkaTemplate<Integer, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public CompletableFuture<SendResult<Integer, String>> sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
        var key = libraryEvent.libraryEventId();
        var value = getObjectAsString(libraryEvent);
        CompletableFuture<SendResult<Integer, String>> completableFuture = kafkaTemplate.send(kafkaTopic, key, value);

        // 1. blocking call - get metadata about the kafka cluster (only the very first call e.g. when starting hte application)
        // 2. send message happens - returns a completable future
        return completableFuture.whenComplete(((sendResult, throwable) -> {
            if (throwable != null) {
                handleFailure(key, value, throwable);
            } else {
                handleSuccess(key, value, sendResult);
            }
        }));
    }

    public SendResult<Integer, String> sendLibraryEventApproachTwo(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        var key = libraryEvent.libraryEventId();
        var value = getObjectAsString(libraryEvent);
        SendResult<Integer, String> sendResult = kafkaTemplate.send(kafkaTopic, key, value)
                //.get();
                .get(3, TimeUnit.SECONDS);

        // 1. blocking call - get metadata about the kafka cluster (only the very first call e.g. when starting hte application)
        // 2. block and wait until the message is sent to kafka

        handleSuccess(key, value, sendResult);
        return sendResult;
    }

    public CompletableFuture<SendResult<Integer, String>> sendLibraryEventApproachThree(LibraryEvent libraryEvent) throws JsonProcessingException {
        var key = libraryEvent.libraryEventId();
        var value = getObjectAsString(libraryEvent);
        ProducerRecord<Integer, String> producerRecord = buildProducerRecord(key, value);
        CompletableFuture<SendResult<Integer, String>> completableFuture = kafkaTemplate.send(producerRecord);

        // 1. blocking call - get metadata about the kafka cluster (only the very first call e.g. when starting hte application)
        // 2. send message happens - returns a completable future
        return completableFuture.whenComplete(((sendResult, throwable) -> {
            if (throwable != null) {
                handleFailure(key, value, throwable);
            } else {
                handleSuccess(key, value, sendResult);
            }
        }));
    }

    private void handleFailure(Integer key, String value, Throwable throwable) {
        log.error("Error sending message and the exception is {}", throwable.getMessage(), throwable);
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> sendResult) {
        String logMsg = "Message sent successfully for the key: {} and the value: {}, partition is {}, producer record headers: {}";
        Headers headers = sendResult.getProducerRecord().headers();
        log.info(logMsg, key, value, sendResult.getRecordMetadata().partition(), headers);
    }

    private <T> String getObjectAsString(T event) throws JsonProcessingException {
        return objectMapper.writeValueAsString(event);
    }

    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value) {
        List<Header> recordHeaders = List.of(new RecordHeader("event-source", "scanner".getBytes()));
        return new ProducerRecord<>(kafkaTopic,null, key, value, recordHeaders);
    }

}
