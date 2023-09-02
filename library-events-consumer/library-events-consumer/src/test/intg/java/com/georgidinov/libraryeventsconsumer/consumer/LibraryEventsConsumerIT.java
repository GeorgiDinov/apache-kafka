package com.georgidinov.libraryeventsconsumer.consumer;

import com.georgidinov.libraryeventsconsumer.entity.LibraryEvent;
import com.georgidinov.libraryeventsconsumer.jpa.FailureRecordRepository;
import com.georgidinov.libraryeventsconsumer.jpa.LibraryEventsRepository;
import com.georgidinov.libraryeventsconsumer.service.LibraryEventsService;
import com.georgidinov.libraryeventsconsumer.util.ConverterUtil;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest
@EmbeddedKafka(topics = {"${spring.kafka.topic}", "${spring.kafka.topic.retry}", "${spring.kafka.topic.dlt}"}, partitions = 3)
@TestPropertySource(properties =
        {
                "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
                "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}",
                "retry.consumer.startup=false"
        }
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class LibraryEventsConsumerIT {

    private final String createEventPath = "events/library-event-create.json";
    private final String updateEventPath = "events/library-event-update.json";

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;
    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;
    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;
    @Autowired
    LibraryEventsRepository libraryEventsRepository;
    @Autowired
    FailureRecordRepository failureRecordRepository;

    @SpyBean
    LibraryEventsConsumer libraryEventsConsumerSpy;
    @SpyBean
    LibraryEventsService libraryEventServiceSpy;

    Consumer<Integer, String> consumer;

    @Value("${spring.kafka.topic.retry}")
    private String retryTopic;
    @Value("${spring.kafka.topic.dlt}")
    private String dltTopic;
    @Value("${spring.kafka.consumer.group-id}")
    private String libraryConsumerGroup;


    @BeforeEach
    void setUp() {
        MessageListenerContainer container = endpointRegistry.getListenerContainers()
                .stream()
                .filter(messageListenerContainer -> libraryConsumerGroup.equals(messageListenerContainer.getGroupId()))
                .toList()
                .get(0);

        ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());

//        for (MessageListenerContainer messageListenerContainer : endpointRegistry.getListenerContainers()) {
//            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
//        }
    }


    @AfterEach
    void tearDown() throws Exception {
        libraryEventsRepository.deleteAll();
    }


    @Test
    void publishNewLibraryEvent() throws IOException, ExecutionException, InterruptedException {
        //given
        LibraryEvent newLibraryEvent = ConverterUtil.getObjectFromClassPath(new ClassPathResource(createEventPath), LibraryEvent.class);
        String eventString = ConverterUtil.getObjectAsString(newLibraryEvent);
        kafkaTemplate.sendDefault(eventString).get();

        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(2, TimeUnit.SECONDS);

        //then
        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        List<LibraryEvent> libraryEventList = (List<LibraryEvent>) libraryEventsRepository.findAll();
        assert libraryEventList.size() == 1;
        libraryEventList.forEach(libraryEvent -> {
            assert libraryEvent.getLibraryEventId() != null;
            assertEquals(123, libraryEvent.getBook().getBookId());
        });
    }

    @Test
    void publishUpdateLibraryEvent() throws IOException, InterruptedException, ExecutionException {
        //given
        LibraryEvent libraryEvent = ConverterUtil.getObjectFromClassPath(new ClassPathResource(createEventPath), LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        LibraryEvent savedLibraryEvent = libraryEventsRepository.save(libraryEvent);

        LibraryEvent updatedEvent = ConverterUtil.getObjectFromClassPath(new ClassPathResource(updateEventPath), LibraryEvent.class);
        String updatedEventStr = ConverterUtil.getObjectAsString(updatedEvent);

        //when
        kafkaTemplate.sendDefault(savedLibraryEvent.getLibraryEventId(), updatedEventStr).get();
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(2, TimeUnit.SECONDS);

        //then
        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        LibraryEvent persistedLibraryEvent = libraryEventsRepository.findById(savedLibraryEvent.getLibraryEventId()).get();
        assertEquals(updatedEvent.getBook().getBookName(), persistedLibraryEvent.getBook().getBookName());
    }

    @Test
    void publishUpdateLibraryEventNullEvent() throws IOException, InterruptedException, ExecutionException {
        //given
        LibraryEvent updateEvent = ConverterUtil.getObjectFromClassPath(new ClassPathResource(updateEventPath), LibraryEvent.class);
        updateEvent.setLibraryEventId(null);
        String updatedEventStr = ConverterUtil.getObjectAsString(updateEvent);

        //when
        kafkaTemplate.sendDefault(updatedEventStr).get();
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);

        //then
        verify(libraryEventsConsumerSpy, times(3)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventServiceSpy, times(3)).processLibraryEvent(isA(ConsumerRecord.class));

        Map<String, Object> configs = KafkaTestUtils.consumerProps("group2", "true", embeddedKafkaBroker);
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, dltTopic);
        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, dltTopic);

        assertEquals(updatedEventStr, consumerRecord.value());

    }

    @Test
    void publishUpdateLibraryEvent999Event() throws IOException, InterruptedException, ExecutionException {
        //given
        LibraryEvent updateEvent = ConverterUtil.getObjectFromClassPath(new ClassPathResource(updateEventPath), LibraryEvent.class);
        updateEvent.setLibraryEventId(999);
        String updatedEventStr = ConverterUtil.getObjectAsString(updateEvent);

        //when
        kafkaTemplate.sendDefault(updatedEventStr).get();
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);

        //then
        verify(libraryEventsConsumerSpy, times(3)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventServiceSpy, times(3)).processLibraryEvent(isA(ConsumerRecord.class));

        Map<String, Object> configs = KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker);
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, retryTopic);
        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, retryTopic);

        assertEquals(updatedEventStr, consumerRecord.value());

    }


    @Test
    void publishUpdateLibraryEventNullEventFailureRecord() throws IOException, InterruptedException, ExecutionException {
        //given
        LibraryEvent updateEvent = ConverterUtil.getObjectFromClassPath(new ClassPathResource(updateEventPath), LibraryEvent.class);
        updateEvent.setLibraryEventId(null);
        String updatedEventStr = ConverterUtil.getObjectAsString(updateEvent);

        //when
        kafkaTemplate.sendDefault(updatedEventStr).get();
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);

        //then
        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        long count = failureRecordRepository.count();
        assertEquals(1L, count);

        failureRecordRepository.findAll().forEach(failureRecord -> System.out.println("Failure Record: " + failureRecord));

    }


}