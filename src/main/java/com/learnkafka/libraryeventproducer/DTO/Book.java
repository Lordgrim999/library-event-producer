package com.learnkafka.libraryeventproducer.DTO;

public record Book(
        Integer bookId,
        String bookName,
        String authorName
) {
}
