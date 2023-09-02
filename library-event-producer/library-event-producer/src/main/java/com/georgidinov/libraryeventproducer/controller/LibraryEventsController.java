package com.georgidinov.libraryeventproducer.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.georgidinov.libraryeventproducer.domain.LibraryEvent;
import com.georgidinov.libraryeventproducer.domain.LibraryEventType;
import com.georgidinov.libraryeventproducer.producer.LibraryEventsProducer;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@Slf4j
@RestController
public class LibraryEventsController {

    private final LibraryEventsProducer libraryEventsProducer;

    public LibraryEventsController(LibraryEventsProducer libraryEventsProducer) {
        this.libraryEventsProducer = libraryEventsProducer;
    }

    @PostMapping("/v1/library-event")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        log.info("Library event: {}", libraryEvent);
        //libraryEventsProducer.sendLibraryEvent(libraryEvent);
        //libraryEventsProducer.sendLibraryEventApproachTwo(libraryEvent);
        libraryEventsProducer.sendLibraryEventApproachThree(libraryEvent);
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @PutMapping("/v1/library-event")
    public ResponseEntity<?> updateLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        log.info("Library event: {}", libraryEvent);
        ResponseEntity<String> validationResultEntity = validateLibraryEvent(libraryEvent);
        if (validationResultEntity != null) {
            return validationResultEntity;
        }
        libraryEventsProducer.sendLibraryEventApproachThree(libraryEvent);
        return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
    }


    private ResponseEntity<String> validateLibraryEvent(LibraryEvent libraryEvent) {
        if (libraryEvent.libraryEventId() == null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please pass the libraryEventId");
        }
        if (!libraryEvent.libraryEventType().equals(LibraryEventType.UPDATE)) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Only " + LibraryEventType.UPDATE.name() + " is supported");
        }
        return null;
    }

}
