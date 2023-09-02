package com.georgidinov.libraryeventsconsumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.georgidinov.libraryeventsconsumer.entity.LibraryEvent;
import com.georgidinov.libraryeventsconsumer.jpa.LibraryEventsRepository;
import com.georgidinov.libraryeventsconsumer.util.ConverterUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Slf4j
@Service
public class LibraryEventsService {

    private final LibraryEventsRepository libraryEventsRepository;


    public LibraryEventsService(LibraryEventsRepository libraryEventsRepository) {
        this.libraryEventsRepository = libraryEventsRepository;
    }


    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        LibraryEvent libraryEvent = ConverterUtil.getObjectFromString(consumerRecord.value(), LibraryEvent.class);
        log.info("LibraryEvent: {}", libraryEvent);

        if (libraryEvent != null &&libraryEvent.getLibraryEventId() != null && libraryEvent.getLibraryEventId() == 999) {
            throw new RecoverableDataAccessException("Temporary Network Issue!");
        }

        switch (libraryEvent.getLibraryEventType()) {
            case NEW -> save(libraryEvent);
            case UPDATE -> {
                validate(libraryEvent);
                save(libraryEvent);
            }
            default -> log.info("Invalid Library Event Type");
        }
    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        log.info("Saving library event: {}", libraryEvent);
        LibraryEvent savedEvent = libraryEventsRepository.save(libraryEvent);
        log.info("Successfully persisted the library event: {}", savedEvent);
    }

    private void validate(LibraryEvent libraryEvent) {
        if (libraryEvent.getLibraryEventId() == null) {
            throw new IllegalArgumentException("Library event ID is missing");
        }
        Optional<LibraryEvent> eventOptional = libraryEventsRepository.findById(libraryEvent.getLibraryEventId());
        if (eventOptional.isEmpty()) {
            throw new IllegalArgumentException("Library event is not valid");
        }
        log.info("Library event validated successfully, event: {}", libraryEvent);
    }

}
