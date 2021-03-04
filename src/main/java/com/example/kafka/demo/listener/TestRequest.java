package com.example.kafka.demo.listener;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class TestRequest {

    private String url;

    private String requestId;

}
