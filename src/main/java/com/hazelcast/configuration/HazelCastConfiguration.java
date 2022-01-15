package com.hazelcast.configuration;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.LocalDateTime;

@Configuration
public class HazelCastConfiguration {

    @Bean
    public com.hazelcast.config.Config hazelCastConfig() {
        return new Config();
    }

    @Bean
    public HazelcastInstance hazelcastInstance(Config hazelCastConfig) {
        return Hazelcast.newHazelcastInstance(hazelCastConfig);
    }

    @Bean
    public IMap<String, LocalDateTime> timeMap(@Qualifier("hazelcastInstance") HazelcastInstance hazelcastInstance) {
        return hazelcastInstance.getMap("hazelcastTimeMap");
    }
}