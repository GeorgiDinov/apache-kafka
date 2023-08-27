package com.georgidinov.libraryeventproducer.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.georgidinov.libraryeventproducer.domain.LibraryEvent;
import com.georgidinov.libraryeventproducer.util.TestUtil;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;


@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = "${spring.kafka.topic}")
@TestPropertySource(
        properties = {
                "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
                "spring.kafka.admin.properties.bootstrap-servers=${spring.embedded.kafka.brokers}",
        }
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class LibraryEventsControllerIT {

    @Autowired
    LibraryEventsController libraryEventsController;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    Consumer<Integer, String> consumer;

    WebTestClient webTestClient;

    @BeforeEach
    void setUp() {
        //embeddedKafkaBroker = new EmbeddedKafkaBroker(1);
        Map<String, Object> configs = KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker);
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);

        webTestClient = WebTestClient.bindToController(libraryEventsController).build();
    }


    @Test
    void postLibraryEvent() {
        //given
        Mono<LibraryEvent> event = Mono.just(TestUtil.libraryEventRecord());
        //when then
        webTestClient
                .post()
                .uri("/v1/library-event")
                .body(event, LibraryEvent.class)
                .exchange()
                .expectStatus()
                .isCreated();

        ConsumerRecords<Integer, String> records = KafkaTestUtils.getRecords(consumer);
        assert records.count() == 1;
        records.forEach(record -> {
            LibraryEvent consumerEvent = TestUtil.parseLibraryEventRecord(new ObjectMapper(), record.value());
            System.out.println("Consumer Event: " + consumerEvent);
            assertEquals(consumerEvent, TestUtil.libraryEventRecord());
        });

    }
}