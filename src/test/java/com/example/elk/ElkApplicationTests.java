package com.example.elk;

import com.example.elk.bean.Article;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.web.bind.annotation.PutMapping;

import javax.swing.text.Highlighter;
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


    //使用文档id查询文档
    @Test
    public void testIdQuery() throws UnknownHostException {
        //1、创建es客户端连接对象
        Settings settings = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9301));
        SearchResponse response = client.prepareSearch("blog3")
                .setTypes("article")
                .setQuery(QueryBuilders.idsQuery().addIds("3"))
                .get();

        SearchHits hits = response.getHits(); //获取命中次数，查询结果有多少对象
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next();//每个查询对象
            System.out.println(searchHit.getSourceAsString()); // 获取字符串格式打印
        }
        //释放资源
        client.close();
    }

    @Test
    public void test9() throws UnknownHostException, JsonProcessingException {
        Settings settings = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9301));
        ObjectMapper objectMapper = new ObjectMapper();
        for (int i = 1; i <= 10; i++) {
            // 描述json 数据
            Article article = new Article();
            article.setId((long) i);
            article.setTitle(i + "搜索工作其实很快乐");
            article.setContent(i
                    + "我们希望我们的搜索解决方案要快，我们希望有一个零配置和一个完全免费的搜索模式，我们希望能够简单地使用JSON通过HTTP的索引数据，我们希望我们的搜索服务器始终可用，我们希望能够一台开始并扩展到数百，我们要实时搜索，我们要简单的多租户，我们希望建立一个云的解决方案。Elasticsearch旨在解决所有这些问题和更多的问题。");

            //建立文档
            client.prepareIndex("blog4", "article", article.getId().toString())
                    .setSource(objectMapper.writeValueAsString(article).getBytes(), XContentType.JSON).get();
        }
        client.close();
    }


    //分页查询
    @Test
    public void test10() throws UnknownHostException {
        //1、创建es客户端连接对象
        Settings settings = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9301));

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("blog4")
                .setTypes("article")
                .setQuery(QueryBuilders.matchAllQuery());//默认10条记录
        //   查询第2页数据，每页20条
        //setFrom()：从第几条开始检索，默认是0。
        //setSize():每页最多显示的记录数。
        searchRequestBuilder.setFrom(0).setSize(5);
        SearchResponse response = searchRequestBuilder.get();

        SearchHits hits = response.getHits(); // 获取命中次数，查询结果有多少对象
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next(); // 每个查询对象
            System.out.println(searchHit.getSourceAsString()); // 获取字符串格式打印
            System.out.println("id:" + searchHit.getSourceAsMap().get("id"));
            System.out.println("title:" + searchHit.getSourceAsMap().get("title"));
            System.out.println("content:" + searchHit.getSourceAsMap().get("content"));
            System.out.println("-----------------------------------------");
        }

        //释放资源
        client.close();
    }



    //高亮显示代码
    @Test
    public void test11() throws UnknownHostException {
        //1、创建es客户端连接对象
        Settings settings = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9301));

        // 搜索数据
        SearchRequestBuilder searchRequestBuilder = client
                .prepareSearch("blog4").setTypes("article")
                .setQuery(QueryBuilders.termQuery("title", "搜索"));

        //设置高亮数据
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<font style='color:red'>");
        highlightBuilder.postTags("</font>");
        highlightBuilder.field("title");
        searchRequestBuilder.highlighter(highlightBuilder);
        //获取查询结果数据
        SearchResponse response = searchRequestBuilder.get();

        //获取查询结果集
        SearchHits searchHits = response.getHits();
        System.out.println("共搜到:"+searchHits.getTotalHits()+"条结果!");
        //遍历结果
        for(SearchHit hit:searchHits){
            System.out.println("String方式打印文档搜索内容:");
            System.out.println(hit.getSourceAsString());
            System.out.println("Map方式打印高亮内容");
            System.out.println(hit.getHighlightFields());

            System.out.println("遍历高亮集合，打印高亮片段:");
            Text[] text = hit.getHighlightFields().get("title").getFragments();
            for (Text str : text) {
                System.out.println(str);
            }
        }

        //释放资源
        client.close();

    }


}
