package com.georgidinov.libraryeventsconsumer.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.georgidinov.libraryeventsconsumer.service.LibraryEventsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class LibraryEventsConsumer {

    private final LibraryEventsService libraryEventsService;

    public LibraryEventsConsumer(LibraryEventsService libraryEventsService) {
        this.libraryEventsService = libraryEventsService;
    }

    @KafkaListener(topics = {"${spring.kafka.topic}"}, groupId = "${spring.kafka.consumer.group-id}")
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        log.info("Consumer record: {}", consumerRecord);
        libraryEventsService.processLibraryEvent(consumerRecord);
    }


}