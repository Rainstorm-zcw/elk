package com.example.elk.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RestClientConfig {

    @Value("${es.clusterName}")
    public static String clusterName;
    @Value("${es.servers}")
    public static String servers;
    @Value("${es.port}")
    public static int port;

    @Bean
    public RestHighLevelClient restHighLevelClient() {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(servers, port, "http")
                )
        );
        return client;
    }

}
