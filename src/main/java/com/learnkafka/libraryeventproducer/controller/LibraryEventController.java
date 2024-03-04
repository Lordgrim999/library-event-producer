package com.learnkafka.libraryeventproducer.controller;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.libraryeventproducer.DTO.LibraryEvent;
import com.learnkafka.libraryeventproducer.DTO.LibraryEventType;
import com.learnkafka.libraryeventproducer.producer.LibraryEventProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class LibraryEventController {

    private final LibraryEventProducer libraryEventProducer;

    public LibraryEventController(LibraryEventProducer libraryEventProducer) {
        this.libraryEventProducer = libraryEventProducer;
    }

    @PostMapping("v1/libraryevent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {
        log.info("library event: {} ",libraryEvent);
        libraryEventProducer.sendLibraryEvent(libraryEvent);
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @PutMapping("v1/libraryevent")
    public ResponseEntity<?> updateLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {
        log.info("library event: {} ",libraryEvent);
        if(libraryEvent.libraryEventId()==null)
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("libraryEventId is missing");
        if(!libraryEvent.libraryEventType().equals(LibraryEventType.UPDATE))
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Only update event type is supported");
        libraryEventProducer.sendLibraryEvent(libraryEvent);
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

}
