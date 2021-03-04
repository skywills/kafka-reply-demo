package com.example.kafka.demo.listener;

import com.example.kafka.demo.enums.MessageChannel;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

import java.util.Random;

@Component
@Slf4j
public class KafkaListener {

    private final ObjectMapper objectMapper;

    public KafkaListener(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @org.springframework.kafka.annotation.KafkaListener(topics = MessageChannel.TEST_REQUEST)
    @SendTo
    public String testRequestListener(String message) {
        TestRequest request;
        log.info("listen TEST.REQUEST message: {}", message);
        try {
            request = objectMapper.readValue(message, TestRequest.class);
        } catch (JsonProcessingException e){
            e.printStackTrace();
            throw new RuntimeException("error processing");
        }

        double rangeMin = 0;
        double rangeMax = 100;
        Random r = new Random();
        double randomValue = rangeMin + (rangeMax - rangeMin) * r.nextDouble();

        TestResponse response = new TestResponse(request.getRequestId(), randomValue);
        try {
            return objectMapper.writeValueAsString(response);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("error write");
        }
    }

    @org.springframework.kafka.annotation.KafkaListener(topics = "TEST.REQUEST.1")
    @SendTo
    public String testRequestListener1(String message) {
        TestRequest request;
        log.info("listen TEST.REQUEST.1 message : {}", message);
        try {
            request = objectMapper.readValue(message, TestRequest.class);
        } catch (JsonProcessingException e){
            e.printStackTrace();
            throw new RuntimeException("error processing");
        }

        double rangeMin = 0;
        double rangeMax = 100;
        Random r = new Random();
        double randomValue = rangeMin + (rangeMax - rangeMin) * r.nextDouble();

        TestResponse response = new TestResponse(request.getRequestId(), randomValue);
        try {
            return objectMapper.writeValueAsString(response);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("error write");
        }
    }

}
