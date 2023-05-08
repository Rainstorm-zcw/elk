package com.example.elk;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.elk.bean.Article;
import com.example.elk.config.RestClientConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.assertj.core.util.Lists;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

@Slf4j
@SpringBootTest
public class Elasticsearch7xTest {

    @Qualifier(value = "restHighLevelClient")
    @Autowired
    public RestHighLevelClient client;

    @Before
    public void before() {
        client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("127.0.0.1", 9200, "http")
        ));

    }

    @After
    public void after() throws IOException {
        this.client.close();
    }


    @Test
    public void testInfo() {
        System.out.println(client);
    }


    //创建索引
    @Test
    public void testCreateDoc() throws IOException {
        Article article = new Article();
        article.setId(1L);
        article.setTitle("测试1");
        article.setContent("这是一个文档");
        IndexRequest request = new IndexRequest("article1");
        //设置索引
        request.id("1");
        //设置超时时间
        request.timeout(TimeValue.timeValueSeconds(5));
        request.source(JSON.toJSONString(article), XContentType.JSON);
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        System.out.println(response);
    }

    //获取索引
    @Test
    public void getDoc() throws IOException {
        GetRequest getRequest = new GetRequest("article1").id("1");
        GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
        System.out.println(response.getSourceAsString());
    }

    //删除索引
    @Test
    public void delDoc() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("article1").id("1");
        DeleteResponse delete = client.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(delete.status());
    }

    //todo  contains unrecognized parameter: [ignore_throttled]] 版本问题
    @Test
    public void delIndex() throws IOException {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("article1");
        AcknowledgedResponse delete = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }

    //批处理添加
    @Test
    public void batchLoad() throws IOException {
        List<Article> list = Lists.newArrayList();
        list.add(new Article(2L, "学习", "每日充电"));
        list.add(new Article(3L, "工作", "兢兢业业工作"));
        list.add(new Article(4L, "job", "find good work job"));
        //创建批量处理对象
        BulkRequest bulkRequest = new BulkRequest();
        for (Article article : list) {
            String s = JSON.toJSONString(article);
            IndexRequest indexRequest = new IndexRequest("article_index").id(article.getId() + "").source(s, XContentType.JSON);
            //IndexRequest indexRequest = new IndexRequest("article1");
            //indexRequest.index(article.getId() + "");
            //indexRequest.source(JSON.toJSONString(article), XContentType.JSON);
            /**
             * 在 5.X 版本中，一个 index 下可以创建多个 type；
             *
             * 在 6.X 版本中，一个 index 下只能存在一个 type；
             *
             * 在 7.X 版本中，直接去除了 type 的概念，就是说 index 不再会有 type。
             */
            indexRequest.type("type_sole");
            System.out.println(JSON.toJSONString(indexRequest));
            bulkRequest.add(indexRequest);
        }

        //提交批量处理对象
        BulkResponse bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        //查看添加状态
        System.out.println(bulk.status());
    }

    /**
     * 查询条件数据 匹配查询不到将字段类似设置为.keyword
     */
    @Test
    public void searchAll() throws IOException {
        SearchRequest searchRequest = new SearchRequest("article_index");
        //制定搜索条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //searchSourceBuilder.query(QueryBuilders.matchAllQuery());//查询所有
        //searchSourceBuilder.query(QueryBuilders.termQuery("title","job")); //非string类型查询
        //searchSourceBuilder.query(QueryBuilders.termQuery("content", "job")); //非string类型查询
        //searchSourceBuilder.query(QueryBuilders.matchPhraseQuery("content","兢兢业业"));//精准匹配string
        //searchSourceBuilder.query(QueryBuilders.matchQuery("content.keyword","job"));//不能匹配到
        searchSourceBuilder.query(QueryBuilders.termQuery("content.keyword", "兢兢业业工作"));
        searchSourceBuilder.sort("id", SortOrder.DESC);
        searchRequest.source(searchSourceBuilder);
        //获得文档对象
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        //获得文档数据
        for (SearchHit hit : search.getHits().getHits()) {
            Article article = JSONObject.parseObject(hit.getSourceAsString(), Article.class);
            System.out.println(JSON.toJSONString(article));
        }

    }

    /**
     * 类似数据库的or操作
     */
    @Test
    public void searchByBolt() throws IOException {
        SearchRequest request = new SearchRequest("article_index");
        SearchSourceBuilder builder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.should(QueryBuilders.matchQuery("id", 4));
        boolQueryBuilder.should(QueryBuilders.matchQuery("title", "工作").boost(10));
        builder.query(boolQueryBuilder);
        request.source(builder);
        SearchResponse search = client.search(request, RequestOptions.DEFAULT);
        for (SearchHit hit : search.getHits().getHits()) {
            Article article = JSONObject.parseObject(hit.getSourceAsString(), Article.class);
            System.out.println(JSON.toJSONString(article));
        }

    }


    /**
     * 查询部分字段
     */
    @Test
    public void searchByParam() throws IOException {
        SearchRequest request = new SearchRequest("article_index");
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());
        String[] includes = {"id", "title"};
        String[] excludes = {};
        builder.fetchSource(includes, excludes);
        request.source(builder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        for (SearchHit hit : response.getHits().getHits()) {
            Article article = JSONObject.parseObject(hit.getSourceAsString(), Article.class);
            System.out.println(JSON.toJSONString(article));
        }
    }

    /**
     * 范围查询 大于小于
     */
    @Test
    public void searchByFilter() throws IOException {
        SearchRequest request = new SearchRequest("article_index");
        SearchSourceBuilder builder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("id").gt(2).lt(4));
        builder.query(boolQueryBuilder);
        request.source(builder);
        log.info("范围查询boolQuery为:{},这个是{}", JSON.toJSONString(request),JSON.toJSONString(boolQueryBuilder));
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        for (SearchHit hit : response.getHits().getHits()) {
            Article article = JSONObject.parseObject(hit.getSourceAsString(), Article.class);
            System.out.println(JSON.toJSONString(article));
        }
    }

    /**
     * 模糊查询
     */
    @Test
    public void searchByLike() throws IOException {
        SearchRequest request = new SearchRequest("article_index");
        SearchSourceBuilder builder = new SearchSourceBuilder();
        TermQueryBuilder termQuery = QueryBuilders.termQuery("title.keyword", "学");
        builder.query(termQuery);
        request.source(builder);
        System.out.println(JSON.toJSONString("输出："+request));
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        for (SearchHit hit : response.getHits().getHits()) {
            Article article = JSONObject.parseObject(hit.getSourceAsString(), Article.class);
            System.out.println(JSON.toJSONString(article));
        }
    }

    /*********************************************高阶查询*************************************************/

    /**
     * 多个匹配条件，满足其中一个即可，类似于sql中的or
     * GET /article_index/_search
     * {
     *   "query":{
     *     "terms": {
     *       "city.keyword": ["武汉","厦门"]
     *     }
     *   }
     * }
     */
    @Test
    public void searchOrAll() throws IOException {
        SearchRequest request = new SearchRequest("article_index");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("title.keyword", "哈哈哈哈", "job");
        //组装
        searchSourceBuilder.query(termsQueryBuilder);
        //执行
        request.source(searchSourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        for (SearchHit hit : response.getHits().getHits()) {
            Article article = JSONObject.parseObject(hit.getSourceAsString(), Article.class);
            System.out.println(JSON.toJSONString(article));
        }
    }

    /**
     * 范围查询
     * GET /architecture_index/_search
     * {
     *   "query":{
     *     "range": {
     *       "price":{
     *         "gte": 50,
     *         "lte": 100
     *       }
     *     }
     *   }
     * }
     */
    @Test
    public void searchRangeFilter() throws IOException {
        SearchRequest request = new SearchRequest("article_index");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.rangeQuery("id").gte(3).lte(2));
        request.source(sourceBuilder);
        log.info("参数为:{}", JSONObject.toJSONString(request));
        SearchResponse search = client.search(request, RequestOptions.DEFAULT);
        for (SearchHit hit : search.getHits().getHits()) {
            Article article = JSON.parseObject(hit.getSourceAsString(), Article.class);
            System.out.println(article);
        }
    }

    /**
     *  不同字段多条件查询（and）
     *  多个匹配条件，同时需要满足，类似于sql中的and
     * bool 可以用来合并多个条件查询结果的布尔逻辑，它包含一下操作符：
     * must : 多个查询条件的完全匹配,相当于 and。
     * must_not : 多个查询条件的相反匹配，相当于 not。
     * should : 至少有一个查询条件匹配, 相当于 or。
     *
     * GET /architecture_index/_search
     * {
     *   "query":{
     *     "bool": {
     *       "must": [
     *         {"term": {
     *           "city.keyword": {
     *             "value": "合肥"
     *           }
     *         }},
     *         {
     *           "range": {
     *             "price": {
     *               "gte": 50,
     *               "lte": 80
     *             }
     *           }}
     *       ]
     *     }
     *   }
     * }
     */
    @Test
    public void searchByBolts() throws IOException {
        SearchRequest request = new SearchRequest("article_index");
        SearchSourceBuilder builder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.termQuery("title","job"));
        boolQueryBuilder.must(QueryBuilders.rangeQuery("id").lte(4).gt(1));
        builder.query(boolQueryBuilder);
        request.source(builder);
        System.out.println(JSON.toJSONString(request));
        SearchResponse search = client.search(request, RequestOptions.DEFAULT);
        log.info("search为:{}", JSON.toJSONString(search));
        for (SearchHit hit : search.getHits().getHits()) {
            Article article = JSON.parseObject(hit.getSourceAsString(), Article.class);
            System.out.println(JSON.toJSONString(article));
        }
    }

    /**
     * 不同字段多条件查询（or）
     * 多个条件匹配，满足一个即可
     * should : 至少有一个查询条件匹配, 相当于 or。
     * GET /article_index/_search
     * {
     *   "query":{
     *     "bool": {
     *       "should": [
     *         {"term": {
     *           "city.keyword": {
     *             "value": "合肥"
     *           }
     *         }
     *         },
     *         {
     *           "range": {
     *             "price": {
     *               "gte": 50,
     *               "lte": 80
     *             }
     *           }
     *         }
     *       ]
     *     }
     *   }
     * }
     */
    @Test
    public void searchOrBolt() throws IOException {
        SearchRequest request = new SearchRequest("article_index");
        SearchSourceBuilder builder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.should(QueryBuilders.termQuery("title","job"));
        boolQueryBuilder.should(QueryBuilders.rangeQuery("id").lte(4).gt(1));
        builder.query(boolQueryBuilder);
        request.source(builder);
        System.out.println(JSON.toJSONString(request));
        SearchResponse search = client.search(request, RequestOptions.DEFAULT);
        log.info("search为:{}", JSON.toJSONString(search));
        for (SearchHit hit : search.getHits().getHits()) {
            Article article = JSON.parseObject(hit.getSourceAsString(), Article.class);
            System.out.println(JSON.toJSONString(article));
        }
    }

}
