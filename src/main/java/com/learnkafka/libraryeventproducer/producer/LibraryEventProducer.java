package com.learnkafka.libraryeventproducer.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.libraryeventproducer.DTO.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
@Slf4j
public class LibraryEventProducer {

    // the kafka template will get configured from the details in the application props file
    private final KafkaTemplate<Integer,String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${spring.kafka.topic}")
    private String topic;

    public LibraryEventProducer(KafkaTemplate<Integer, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public CompletableFuture<SendResult<Integer, String>> sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
        var key=libraryEvent.libraryEventId();
        var value=objectMapper.writeValueAsString(libraryEvent);
        var completableFuture = kafkaTemplate.send(topic,key,value);
        return completableFuture.whenComplete((sendResult,throwable)->{
           if(throwable!=null)
           {
                handleFailure(key,value,throwable);
           }
           else{
               handleSuccess(key,value,sendResult);
           }
        });
    }

    public CompletableFuture<SendResult<Integer, String>> sendLibraryEventApproach2(LibraryEvent libraryEvent) throws JsonProcessingException {
        var key=libraryEvent.libraryEventId();
        var value=objectMapper.writeValueAsString(libraryEvent);
        var producerRecord=buildProducerRecord(key,value);
        var completableFuture = kafkaTemplate.send(producerRecord);
        return completableFuture.whenComplete((sendResult,throwable)->{
            if(throwable!=null)
            {
                handleFailure(key,value,throwable);
            }
            else{
                handleSuccess(key,value,sendResult);
            }
        });
    }

    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value) {

        return new ProducerRecord<Integer,String>(topic,key,value);
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> sendResult) {

        log.info("Message sent successfully for the key: {} and the value: {}, partition is {}",key,value,sendResult.getRecordMetadata().partition());
    }

    private void handleFailure(Integer key, String value, Throwable throwable) {

        log.error("Error occurred while sending message and the exception is {}",throwable.getMessage(),throwable);

    }
}
