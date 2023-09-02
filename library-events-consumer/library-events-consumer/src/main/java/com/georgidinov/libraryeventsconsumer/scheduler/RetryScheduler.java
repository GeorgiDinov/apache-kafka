package com.georgidinov.libraryeventsconsumer.scheduler;

import com.georgidinov.libraryeventsconsumer.entity.FailedRecordStatus;
import com.georgidinov.libraryeventsconsumer.entity.FailureRecord;
import com.georgidinov.libraryeventsconsumer.jpa.FailureRecordRepository;
import com.georgidinov.libraryeventsconsumer.service.LibraryEventsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class RetryScheduler {


    private final FailureRecordRepository failureRecordRepository;
    private final LibraryEventsService libraryEventsService;

    public RetryScheduler(FailureRecordRepository failureRecordRepository, LibraryEventsService libraryEventsService) {
        this.failureRecordRepository = failureRecordRepository;
        this.libraryEventsService = libraryEventsService;
    }

    @Scheduled(fixedRate = 10_000L)
    public void retryFailedRecords() {
        log.info("Retrying failed records");
        failureRecordRepository.findAllByStatus(FailedRecordStatus.RETRY.name()).forEach(failureRecord -> {
            log.info("FailedRecord: {}", failureRecord);
            ConsumerRecord<Integer, String> consumerRecord = buildConsumerRecord(failureRecord);
            try {
                libraryEventsService.processLibraryEvent(consumerRecord);
                failureRecord.setStatus(FailedRecordStatus.SUCCESS.name());
                failureRecordRepository.save(failureRecord);
            } catch (Exception exception) {
                log.info("Exception in retryFailedRecords: {}", exception.getMessage(), exception);
            }
        });

    }

    private ConsumerRecord<Integer, String> buildConsumerRecord(FailureRecord failureRecord) {
        return new ConsumerRecord<>
                (
                        failureRecord.getTopic(),
                        failureRecord.getPartition(),
                        failureRecord.getOffset_value(),
                        failureRecord.getKey_value(),
                        failureRecord.getErrorRecord()
                );
    }


}
