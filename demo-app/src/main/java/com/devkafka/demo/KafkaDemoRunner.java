package com.devkafka.demo;

import com.devkafka.client.KafkaRestProxyClientService;
import com.devkafka.runner.GenericRunnerBean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class KafkaDemoRunner implements CommandLineRunner {

    private final GenericRunnerBean genericRunnerBean;
    private final KafkaRestProxyClientService restProxyClient;
    private final DemoKafkaProperties properties;
    private final String schemaRegistryUrl;

    public KafkaDemoRunner(GenericRunnerBean genericRunnerBean,
                            KafkaRestProxyClientService restProxyClient,
                            DemoKafkaProperties properties,
                            @Value("${library.schema.registry-url}") String schemaRegistryUrl) {
        this.genericRunnerBean = genericRunnerBean;
        this.restProxyClient = restProxyClient;
        this.properties = properties;
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("Existing REST Proxy topics: {}", restProxyClient.listTopics(properties.getRestProxyUrl()));

        if (!properties.isSendSampleMessage()) {
            log.info("app.kafka.send-sample-message=false, skipping schema download and sample message send.");
            return;
        }

        genericRunnerBean.run(
                properties.getKeyPayloadFile(),
                properties.getValuePayloadFile(),
                schemaRegistryUrl,
                properties.getTopicName(),
                properties.getRestProxyUrl(),
                properties.getSchemaUrlSuffix()
        );
    }
}
