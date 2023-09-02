package com.georgidinov.libraryeventsconsumer.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.georgidinov.libraryeventsconsumer.service.LibraryEventsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class LibraryEventsRetryConsumer {


    private final LibraryEventsService libraryEventsService;

    public LibraryEventsRetryConsumer(LibraryEventsService libraryEventsService) {
        this.libraryEventsService = libraryEventsService;
    }

    @KafkaListener
            (
                    topics = {"${spring.kafka.topic.retry}"}, groupId = "${kafka.retry.listener.group}",
                    autoStartup = "${retry.consumer.startup:true}"
            )
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        log.info("Retry consumer record: {}", consumerRecord);
        consumerRecord.headers().forEach(header -> {
            log.info("Retry consumer record headers:");
            log.info("\tKey: {}, Value: {}", header.key(), new String(header.value()));
        });
        libraryEventsService.processLibraryEvent(consumerRecord);
    }


}
