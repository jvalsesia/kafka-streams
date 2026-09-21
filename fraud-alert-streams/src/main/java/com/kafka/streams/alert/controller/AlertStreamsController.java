package com.kafka.streams.alert.controller;

import org.apache.kafka.streams.KafkaStreams;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/alerts")
public class AlertStreamsController {

    private final StreamsBuilderFactoryBean streamsFactory;

    public AlertStreamsController(StreamsBuilderFactoryBean streamsFactory) {
        this.streamsFactory = streamsFactory;
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getStreamsStatus() {
        KafkaStreams.State state = KafkaStreams.State.NOT_RUNNING;
        KafkaStreams streams = streamsFactory.getKafkaStreams();
        if (streams != null) {
            state = streams.state();
        }

        return ResponseEntity.ok(Map.of(
                "service", "fraud-alert-streams",
                "streamsRunning", streamsFactory.isRunning(),
                "streamsState", state.name()
        ));
    }
}
