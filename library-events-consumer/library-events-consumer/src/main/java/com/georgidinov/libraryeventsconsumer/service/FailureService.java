package com.georgidinov.libraryeventsconsumer.service;

import com.georgidinov.libraryeventsconsumer.entity.FailedRecordStatus;
import com.georgidinov.libraryeventsconsumer.entity.FailureRecord;
import com.georgidinov.libraryeventsconsumer.jpa.FailureRecordRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class FailureService {

    private final FailureRecordRepository failureRecordRepository;

    public FailureService(FailureRecordRepository failureRecordRepository) {
        this.failureRecordRepository = failureRecordRepository;
    }


    public void saveFailedRecord(ConsumerRecord<Integer, String> consumerRecord,
                                 Exception exception,
                                 FailedRecordStatus failedRecordStatus) {

        FailureRecord failureRecord = new FailureRecord();
        failureRecord.setId(null);
        failureRecord.setTopic(consumerRecord.topic());
        failureRecord.setKey_value(consumerRecord.key());
        failureRecord.setPartition(consumerRecord.partition());
        failureRecord.setOffset_value(consumerRecord.offset());
        failureRecord.setException(exception.getCause().getMessage());
        failureRecord.setStatus(failedRecordStatus.name());

        FailureRecord savedFailureRecord = failureRecordRepository.save(failureRecord);
        log.info("Saved failure record: {}", savedFailureRecord);
    }


}
