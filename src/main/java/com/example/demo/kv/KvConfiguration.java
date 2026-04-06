package com.example.demo.kv;

import io.tarantool.client.box.TarantoolBoxClient;
import io.tarantool.client.factory.TarantoolBoxClientBuilder;
import io.tarantool.client.factory.TarantoolFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(TarantoolProperties.class)
public class KvConfiguration {

    @Bean(destroyMethod = "close")
    TarantoolBoxClient tarantoolBoxClient(TarantoolProperties properties) throws Exception {
        TarantoolBoxClientBuilder builder = TarantoolFactory.box()
                .withHost(properties.getHost())
                .withPort(properties.getPort())
                .withUser(properties.getUser());

        if (!"guest".equals(properties.getUser())) {
            builder.withPassword(properties.getPassword());
        }
        return builder.build();
    }

    @Bean
    KvStorage kvStorage(TarantoolBoxClient client, TarantoolProperties properties) {
        return new TarantoolKvStorage(client, properties.getSpace(), properties.getRangeBatchSize());
    }
}
