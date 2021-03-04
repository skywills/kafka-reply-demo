package com.example.kafka.demo.listener;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class TestResponse {

    public TestResponse(String requestId, double score) {
        this.requestId = requestId;
        this.score = score;
    }

    private String requestId;

    private double score;

}
