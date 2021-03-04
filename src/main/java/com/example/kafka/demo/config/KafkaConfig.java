package com.example.kafka.demo.config;

import com.example.kafka.demo.enums.MessageChannel;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;

import java.util.Map;

@Configuration
public class KafkaConfig {

    // ReplyingKafkaTemplate
    @Bean
    public ReplyingKafkaTemplate<String, Object, Object> replyKafkaTemplate(ProducerFactory<String, Object> producerFactory, KafkaMessageListenerContainer<String, Object> container) {
        return new ReplyingKafkaTemplate<>(producerFactory, container);
    }

    // Listener Container to be set up in ReplyingKafkaTemplate
    @Bean
    public KafkaMessageListenerContainer<String, Object> replyContainer(ConsumerFactory<String, Object> consumerFactory) {
        ContainerProperties containerProperties = new ContainerProperties(MessageChannel.REPLY_TOPIC);
        return new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
    }

    // Default Producer Factory to be used in ReplyingKafkaTemplate
    @Bean
    public ProducerFactory<String,Object> producerFactory(KafkaProperties properties) {
        Map<String, Object> producerProperties = properties.buildProducerProperties();
        return new DefaultKafkaProducerFactory<>(producerProperties);
    }

    // Concurrent Listener container factory
    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Object>> kafkaListenerContainerFactory(KafkaTemplate<String, Object> kafkaTemplate,
                                                                                                                           ConsumerFactory<String, Object> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        // NOTE - set up of reply template
        factory.setReplyTemplate(kafkaTemplate);
        return factory;
    }

    @Bean
    public ConsumerFactory<String, Object> consumerFactory(KafkaProperties properties) {
        Map<String, Object> consumerProperties = properties.buildConsumerProperties();
        return new DefaultKafkaConsumerFactory<>(consumerProperties);
    }

    // Standard KafkaTemplate
    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String,Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

}
