package com.learnkafka.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.config.LibraryEventsConsumerConfig;
import com.learnkafka.entity.FailureRecord;
import com.learnkafka.jpa.FailureRecordRepository;
import com.learnkafka.service.LibraryEventService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class RetryScheduler {

    private FailureRecordRepository failureRecordRepository;
    private LibraryEventService libraryEventService;

    public RetryScheduler(FailureRecordRepository failureRecordRepository, LibraryEventService libraryEventService) {
        this.failureRecordRepository = failureRecordRepository;
        this.libraryEventService = libraryEventService;
    }

    @Scheduled(fixedRate = 10000)
    public void retryFailedRecords(){

        log.info("Retrying Failed Records Started!");

        failureRecordRepository.findAllByStatus(LibraryEventsConsumerConfig.RETRY)
        .forEach(failureRecord -> {
            log.info("Retrying Failed Records : {}", failureRecord);
            var consumerRecord = buildConsumerRecord(failureRecord);
            try {
                libraryEventService.processLibraryEvent(consumerRecord);
                failureRecord.setStatus(LibraryEventsConsumerConfig.SUCCESS);
            } catch (Exception e) {
                log.error("Exception in retryFailedRecords : {}", e.getMessage(), e);
            }
        });

        log.info("Retrying Failed Records Completed!");

    }

    private ConsumerRecord<Integer, String> buildConsumerRecord(FailureRecord failureRecord) {
        return new ConsumerRecord<>(
                failureRecord.getTopic(),
                failureRecord.getPartition(),
                failureRecord.getOffset_value(),
                failureRecord.getKey(),
                failureRecord.getErrorRecord()
        );
    }

}
