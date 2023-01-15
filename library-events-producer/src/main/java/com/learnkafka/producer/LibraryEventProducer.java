package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
@Slf4j
public class LibraryEventProducer {

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    String topic = "library-events";
    @Autowired
    ObjectMapper objectMapper;

    public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {

        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        CompletableFuture<SendResult<Integer, String>> completableFuture = kafkaTemplate.sendDefault(key, value);
        completableFuture.whenComplete((result, ex) -> {
            if (ex != null) {
                handleFailure(key, value, ex);
            } else {
                handleSuccess(key, value, result);
            }
        });
    }

    public CompletableFuture<SendResult<Integer, String>> sendLibraryEvent_Approach2(LibraryEvent libraryEvent) throws JsonProcessingException {

        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        ProducerRecord<Integer, String> producerRecord = buildProducerRecord(key, value, topic);
        CompletableFuture<SendResult<Integer, String>> completableFuture = kafkaTemplate.send(producerRecord);
        completableFuture.whenComplete((result, ex) -> {
            if (ex != null) {
                handleFailure(key, value, ex);
            } else {
                handleSuccess(key, value, result);
            }
        });
        return completableFuture;
    }

    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topic) {

        List<Header> recordHeaders = List.of(new RecordHeader("event-source", "scanner".getBytes()),
                new RecordHeader("event-source", "scanner".getBytes()));

        return new ProducerRecord<>(topic, null, key, value, recordHeaders);
    }

    public SendResult<Integer, String> sendLibraryEventSynchronous(LibraryEvent libraryEvent) throws ExecutionException, InterruptedException, JsonProcessingException, TimeoutException {

        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        SendResult<Integer, String> sendResult = null;

        try {
            sendResult = kafkaTemplate.sendDefault(key, value).get(1, TimeUnit.SECONDS);
        } catch (ExecutionException | InterruptedException e) {
            log.error("ExecutionException/InterruptedException Error Sending the Message and the exception is {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Exception Error Sending the Message and the exception is {}", e.getMessage());
            throw e;
        }
        return sendResult;
    }

    private void handleFailure(Integer key, String value, Throwable ex) {
        log.error("Error Sending the Message and the exception is {}", ex.getMessage());
        try {
            throw ex;
        } catch (Throwable throwable) {
            log.error("Error in OnFailure: {}", throwable.getMessage());
        }

    }

    private void handleSuccess(Integer key, String value, SendResult<Integer,String> result) {
        log.info("Message Sent Successfully for the key: {} and the value is {}, partition is {}", key, value, result.getRecordMetadata().partition());
    }

}
