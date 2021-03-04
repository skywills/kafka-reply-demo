package com.example.kafka.demo.web;

import com.example.kafka.demo.enums.MessageChannel;
import com.example.kafka.demo.listener.TestRequest;
import com.example.kafka.demo.listener.TestResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springside.modules.utils.base.ExceptionUtil;

@RestController
@Slf4j
public class WebController {

    private final ObjectMapper objectMapper;

    private final ReplyingKafkaTemplate<String, Object, Object> replyKafkaTemplate;

    public WebController(ObjectMapper objectMapper, ReplyingKafkaTemplate<String, Object, Object> replyKafkaTemplate) {
        this.objectMapper = objectMapper;
        this.replyKafkaTemplate = replyKafkaTemplate;
    }

    @GetMapping(value = "/test-produce")
    public Object testProduce() {

        try{
            String channel = MessageChannel.TEST_REQUEST;
            TestRequest testRequest = new TestRequest();
            testRequest.setRequestId("1000001");
            testRequest.setUrl("http://localhost/image");
            String valueJson = this.objectMapper.writeValueAsString(testRequest);
            // create producer record
            ProducerRecord<String, Object> record = new ProducerRecord<>(channel, valueJson);
            // set reply topic in header
            record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, MessageChannel.REPLY_TOPIC.getBytes()));
            // post in kafka topic
            RequestReplyFuture<String, Object, Object> sendAndReceive = replyKafkaTemplate.sendAndReceive(record);

            // confirm if producer produced successfully
            SendResult<String, Object> sendResult = sendAndReceive.getSendFuture().get();

            //print all headers
            sendResult.getProducerRecord().headers().forEach(header -> System.out.println("header:" + header.key() + ":" + header.value().toString()));

            // get consumer record
            ConsumerRecord<String, Object> consumerRecord = sendAndReceive.get();
            // return consumer value
            return objectMapper.readValue( consumerRecord.value().toString(), TestResponse.class);
        }catch (Exception ex){
            log.error("发送异常{}", ExceptionUtil.stackTraceText(ex));
            return ExceptionUtil.stackTraceText(ex);
        }
    }


    @GetMapping(value = "/test-produce-1")
    public Object testProduce1() {

        try{
            String channel = MessageChannel.TEST_REQUEST_1;
            TestRequest testRequest = new TestRequest();
            testRequest.setRequestId("1000001-1");
            testRequest.setUrl("http://localhost/image");
            String valueJson = this.objectMapper.writeValueAsString(testRequest);
            // create producer record
            ProducerRecord<String, Object> record = new ProducerRecord<>(channel, valueJson);
            // set reply topic in header
            record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, MessageChannel.REPLY_TOPIC.getBytes()));
            // post in kafka topic
            RequestReplyFuture<String, Object, Object> sendAndReceive = replyKafkaTemplate.sendAndReceive(record);

            // confirm if producer produced successfully
            SendResult<String, Object> sendResult = sendAndReceive.getSendFuture().get();

            //print all headers
            sendResult.getProducerRecord().headers().forEach(header -> System.out.println("header:" + header.key() + ":" + header.value().toString()));

            // get consumer record
            ConsumerRecord<String, Object> consumerRecord = sendAndReceive.get();
            // return consumer value
            return objectMapper.readValue( consumerRecord.value().toString(), TestResponse.class);
        }catch (Exception ex){
            log.error("发送异常{}", ExceptionUtil.stackTraceText(ex));
            return ExceptionUtil.stackTraceText(ex);
        }
    }
}
