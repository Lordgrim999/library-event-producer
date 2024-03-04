package com.learnkafka.libraryeventproducer.DTO;

public record LibraryEvent(
        Integer libraryEventId,
        LibraryEventType libraryEventType,
        Book book
) {
}
