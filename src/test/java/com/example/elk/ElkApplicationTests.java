package com.example.elk;

import com.example.elk.bean.Article;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.web.bind.annotation.PutMapping;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Iterator;

@SpringBootTest
class ElkApplicationTests {

    @Test
    void contextLoads() {
    }


    @Test
    public void test1() throws Exception {
        Settings setting = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        TransportClient client = new PreBuiltTransportClient(setting)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9301));
        client.admin().indices().prepareCreate("blog4").get();
        client.close();
    }


    @Test
    public void test3() throws Exception {
        Settings setting = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        TransportClient client = new PreBuiltTransportClient(setting)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9301));
        // 添加映射
        /**
         * 格式：
         * "mappings" : {
         "article" : {
         "dynamic" : "false",
         "properties" : {
         "id" : { "type" : "string" },
         "content" : { "type" : "string" },
         "author" : { "type" : "string" }
         }
         }
         }
         */
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("article")
                .startObject("properties")
                .startObject("id")
                .field("type", "integer").field("store", true)
                .endObject()
                .startObject("title")
                .field("type", "text").field("store", true).field("analyzer", "ik_smart")
                .endObject()
                .startObject("content")
                .field("type", "text").field("store", true).field("analyzer", "ik_smart")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        //创建映射
        PutMappingRequest mapping = Requests.putMappingRequest("blog4")
                .type("article").source(builder);
        client.admin().indices().putMapping(mapping).get();
        client.close();

    }


    @Test
    public void test4() throws Exception {
        Settings setting = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        TransportClient client = new PreBuiltTransportClient(setting)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9301));
        //创建文档信息
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("id", 1)
                .field("title", "ElasticSearch是一个基于Lucene的搜索服务器")
                .field("content",
                        "它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。Elasticsearch是用Java开发的，并作为Apache许可条款下的开放源码发布，是当前流行的企业级搜索引擎。设计用于云计算中，能够达到实时搜索，稳定，可靠，快速，安装使用方便。")
                .endObject();
        // 建立文档对象
        /**
         * 参数一blog1：表示索引对象
         * 参数二article：类型
         * 参数三1：建立id
         */
        client.prepareIndex("blog4", "article", "1").setSource(builder).get();
        client.close();
    }

    @Test
    public void test5() throws Exception {
        //建立client对象
        Settings settings = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9301));
        // 描述json 数据
        //{id:xxx, title:xxx, content:xxx}
        Article article = new Article();
        article.setId(2L);
        article.setTitle("搜索工作其实很快乐");
        article.setContent("我们希望我们的搜索解决方案要快，我们希望有一个零配置和一个完全免费的搜索模式，我们希望能够简单地使用JSON通过HTTP的索引数据，我们希望我们的搜索服务器始终可用，我们希望能够一台开始并扩展到数百，我们要实时搜索，我们要简单的多租户，我们希望建立一个云的解决方案。Elasticsearch旨在解决所有这些问题和更多的问题。");

        ObjectMapper objectMapper = new ObjectMapper();
        //建立文档
        client.prepareIndex("blog4", "article", article.getId().toString())
                .setSource(objectMapper.writeValueAsString(article).getBytes(), XContentType.JSON).get();
        //释放资源
        client.close();
    }

    @Test
    public void testTempQuery() throws UnknownHostException, UnsupportedEncodingException {
        Settings settings = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9301));
        //设置搜索条件
        SearchResponse searchResponse = client.prepareSearch("blog2")
                .setTypes("article")
                .setQuery(QueryBuilders.termQuery("content", "搜索")).get();
        //遍历搜索结果数据
        SearchHits hits = searchResponse.getHits(); //获取命中次数，查询结果有多少对象
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next();//每个查询对象
            System.out.println(searchHit.getSourceAsString()); // 获取字符串格式打印
            System.out.println("title:" + searchHit.getSourceAsMap().get("title"));
        }
        //释放资源
        client.close();

    }


    @Test
    public void testStringQuery() throws UnknownHostException, UnsupportedEncodingException {
        Settings settings = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9301));
        //设置搜索条件
        SearchResponse searchResponse = client.prepareSearch("blog2")
                .setTypes("article")
                .setQuery(QueryBuilders.queryStringQuery("搜索")).get();
        //遍历搜索结果数据
        SearchHits hits = searchResponse.getHits(); //获取命中次数，查询结果有多少对象
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next();//每个查询对象
            System.out.println(searchHit.getSourceAsString()); // 获取字符串格式打印
            System.out.println("title:" + searchHit.getSourceAsMap().get("title"));
        }
        //释放资源
        client.close();

    }


}
