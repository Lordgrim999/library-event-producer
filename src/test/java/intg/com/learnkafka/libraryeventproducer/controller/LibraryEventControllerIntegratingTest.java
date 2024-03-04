package com.learnkafka.libraryeventproducer.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.libraryeventproducer.DTO.LibraryEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;
import util.TestUtil;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;


@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}"})
@EmbeddedKafka(topics = "library-events")
class LibraryEventControllerIntegratingTest {

    @Autowired
    TestRestTemplate restTemplate;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    private Consumer<Integer, String> consumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
    }
    @Test
    void postLibraryEvent() {
        HttpHeaders headers=new HttpHeaders();
        headers.add("content-type", MediaType.APPLICATION_JSON_VALUE);
        LibraryEvent libraryEvent = TestUtil.libraryEventRecord();
        HttpEntity entity =new HttpEntity<>(libraryEvent,headers);

        ResponseEntity<LibraryEvent> result = restTemplate.exchange("v1/libraryevent",
                HttpMethod.POST, entity, LibraryEvent.class);
        assertEquals(HttpStatus.CREATED,result.getStatusCode());

        //Instantiate a consumer
        // Read the record , assert the count and parse the record and assert on it.

        ConsumerRecords<Integer, String> consumerRecords = KafkaTestUtils.getRecords(consumer);
        //Thread.sleep(3000);
        assert consumerRecords.count() == 1;
        consumerRecords.forEach(record -> {
            var libraryEventActual = TestUtil.parseLibraryEventRecord(objectMapper, record.value());
            assertEquals(libraryEvent, libraryEventActual);

        });

    }

}