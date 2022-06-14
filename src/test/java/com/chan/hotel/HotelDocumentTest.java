package com.chan.hotel;

import cn.hutool.json.JSONUtil;
import com.chan.hotel.mapper.HotelMapper;
import com.chan.hotel.pojo.Hotel;
import com.chan.hotel.pojo.HotelDoc;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * 文档操作的基本步骤：
 * <p>
 * - 初始化RestHighLevelClient
 * - 创建XxxRequest。XXX是Index、Get、Update、Delete、Bulk
 * - 准备参数（Index、Update、Bulk时需要）
 * - 发送请求。调用RestHighLevelClient#.xxx()方法，xxx是index、get、update、delete、bulk
 * - 解析结果（Get时需要）
 *
 * @author CHAN
 * @since 2022/6/13
 */
@SpringBootTest
public class HotelDocumentTest {

    @Resource
    private HotelMapper hotelMapper;

    private RestHighLevelClient client;

    @BeforeEach
    void setUp() {
        // 配置建立http连接信息
        HttpHost httpHost = new HttpHost("localhost", 9200, "http");
        // 辅助构建RestClient
        RestClientBuilder builder = RestClient.builder(httpHost);
        // 创建ES高级客户端对象，进一步封装了RestClient，并允许发起请求和读取响应
        this.client = new RestHighLevelClient(builder);
    }

    @AfterEach
    void tearDown() throws IOException {
        this.client.close();
    }


    /**
     * 向es中hotel索引库新增一条文档，IndexRequest
     */
    @Test
    void testAddDocument() throws IOException {
        Hotel hotel = hotelMapper.selectById(61083L);
        HotelDoc hotelDoc = new HotelDoc(hotel);
        // 将HotelDoc转换为json
        String json = JSONUtil.toJsonStr(hotelDoc);
        // 1.准备Request对象
        IndexRequest request = new IndexRequest("hotel").id(hotelDoc.getId().toString());
        // 2.准备Json文档
        request.source(json, XContentType.JSON);
        // 3.发送请求
        client.index(request, RequestOptions.DEFAULT);
    }

    /**
     * 按文档id查询文档，GetRequest
     */
    @Test
    void testGetDocumentById() throws IOException {
        // 1.准备Request
        GetRequest request = new GetRequest("hotel", "61083");
        // 2.发送请求，得到响应
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        // 3.解析响应结果
        String json = response.getSourceAsString();
        HotelDoc hotelDoc = JSONUtil.toBean(json, HotelDoc.class);
        System.out.println(hotelDoc);
    }

    /**
     * 删除文档，DeleteRequest
     */
    @Test
    void testDeleteDocument() throws IOException {
        // 1.准备Request
        DeleteRequest request = new DeleteRequest("hotel", "61083");
        // 2.发送请求
        client.delete(request, RequestOptions.DEFAULT);
    }

    /**
     * 修改文档，UpdateRequest
     * <p>
     * 在RestClient的API中，全量修改与新增的API完全一致，判断依据是ID：
     * - 如果新增时，ID已经存在，则修改
     * - 如果新增时，ID不存在，则新增
     */
    @Test
    void testUpdateDocument() throws IOException {
        // 1.准备Request
        UpdateRequest request = new UpdateRequest("hotel", "61083");
        // 2.准备请求参数
        request.doc(
                "price", "952",
                "starName", "四钻"
        );
        // 3.发送请求
        client.update(request, RequestOptions.DEFAULT);
    }

    /**
     * 批量导入文档，BulkRequest(批量操作)
     */
    @Test
    void testBulkRequest() throws IOException {
        List<Hotel> hotels = hotelMapper.selectList(null);

        // 1.创建Request
        BulkRequest request = new BulkRequest();

        if (!CollectionUtils.isEmpty(hotels)) {
            hotels.forEach((hotel) -> {
                HotelDoc hotelDoc = new HotelDoc(hotel);
                // 2.构建request
                request.add(new IndexRequest("hotel")
                        .id(hotelDoc.getId().toString())
                        .source(JSONUtil.toJsonStr(hotelDoc), XContentType.JSON));
            });
        }
        // 3.发送请求
        client.bulk(request, RequestOptions.DEFAULT);
    }

    // ----------------------- DSL 查询文档 -----------------------

    /**
     * 查询所有,match_all
     */
    @Test
    void testMatchAll() throws IOException {
        // 1.准备Request
        SearchRequest request = new SearchRequest("hotel");
        // 2.准备DSL
        // SearchSourceBuilder searchSourceBuilder = request.source();
        request.source().query(QueryBuilders.matchAllQuery());
        // 3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.解析响应
        handleResponse(response);
    }

    /**
     * 解析响应
     *
     * @param response SearchResponse
     */
    private void handleResponse(SearchResponse response) {
        // 解析响应
        SearchHits searchHits = response.getHits();
        // 获取总条数
        long total = searchHits.getTotalHits().value;
        System.out.println("共搜索到 " + total + " 条数据。");
        // 获取文档数组
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            // 获取文档source
            String json = hit.getSourceAsString();
            // 反序列化
            HotelDoc hotelDoc = JSONUtil.toBean(json, HotelDoc.class);
            // 获取高亮结果
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            if (!CollectionUtils.isEmpty(highlightFields)) {
                // 根据字段名获取高亮结果
                HighlightField highlightField = highlightFields.get("name");
                if (highlightField != null) {
                    // 获取高亮值
                    String name = highlightField.getFragments()[0].string();
                    // 覆盖非高亮结果
                    hotelDoc.setName(name);
                }
            }
            System.out.println("hotelDoc = " + hotelDoc);
        }
    }


    /**
     * match查询,相当于模糊匹配那种
     * 全文检索的match和multi_match查询与match_all的api基本一致,差别是查询条件,也就是query的部分.
     */
    @Test
    void testMatch() throws IOException {
        // 1.准备Request
        SearchRequest request = new SearchRequest("hotel");
        // 2.准备DSL
        request.source().query(QueryBuilders.matchQuery("all", "如家"));
        // 3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.解析响应
        handleResponse(response);
    }

    /**
     * 精确查询
     * term：词条精确匹配
     */
    @Test
    void testTerm() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");
        // 也可以这样写
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.termQuery("city", "北京"));
        sourceBuilder.size(5);
        searchRequest.source(sourceBuilder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        handleResponse(response);
    }

    /**
     * range：范围查询
     */
    @Test
    void testRange() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.rangeQuery("price").gte(100).lte(1000));
        sourceBuilder.size(20);
        searchRequest.source(sourceBuilder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        handleResponse(response);
    }

    /**
     * 布尔查询是复合查询，布尔查询是用must、must_not、filter等方式组合其它查询
     * 布尔查询是一个或多个查询子句的组合，每一个子句就是一个**子查询**。子查询的组合方式有：
     * - must：必须匹配子查询，类似“与”
     * - should：选择性匹配子查询，类似“或”
     * - must_not：必须不匹配，**不参与算分**，类似“非”
     * - filter：必须匹配，**不参与算分**
     */
    @Test
    void testBool() throws IOException {
        // 1.准备Request
        SearchRequest request = new SearchRequest("hotel");

        // 2.准备DSL
        // 2.1.准备BooleanQuery
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        // 2.2.添加term
        boolQuery.must(QueryBuilders.termQuery("city", "杭州"));
        // 2.3.添加range
        boolQuery.filter(QueryBuilders.rangeQuery("price").lte(250));
        request.source().query(boolQuery);

        // 3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.解析响应
        handleResponse(response);
    }

    /**
     * 排序、分页
     */
    @Test
    void testPageAndSort() throws IOException {
        // 页码，每页大小
        int page = 1, size = 5;
        // 1.准备Request
        SearchRequest request = new SearchRequest("hotel");

        // 2.准备DSL
        // 2.1.query
        request.source().query(QueryBuilders.matchAllQuery());
        // 2.2.排序 sort
        // request.source().sort("price", SortOrder.ASC);或
        request.source().sort(SortBuilders.fieldSort("price").order(SortOrder.ASC));
        // 2.3.分页 from、size
        request.source().from((page - 1) * size).size(5);
        // 3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.解析响应
        handleResponse(response);

    }

    /**
     * 高亮：
     * - 查询的DSL：其中除了查询条件、排序、分页等，还需要添加高亮条件，同样是与query同级。
     * - 结果解析：结果除了要解析_source文档数据，还要解析高亮结果
     * 注：高亮查询必须使用全文检索查询，并且要有搜索关键字，将来才可以对关键字高亮。
     */
    @Test
    void testHighlight() throws IOException {
        // 1.准备Request
        SearchRequest request = new SearchRequest("hotel");
        // 2.准备DSL
        // 2.1.query
        request.source().query(QueryBuilders.matchQuery("all", "如家"));
        // 2.2.高亮
        request.source().highlighter(new HighlightBuilder().field("name").requireFieldMatch(false));
        // 3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.解析响应
        handleResponse(response);

    }


    /**
     * 聚合，例如不查数据，只按照品牌聚合（分组），得到每个品牌有多少家酒店
     */
    @Test
    void testAgg() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");
        searchRequest.source().aggregation(AggregationBuilders.terms("brand_agg").field("brand").size(20));
        searchRequest.source().size(0);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        // 解析聚合结果，先获取所有聚合
        Aggregations aggregations = response.getAggregations();
        // 根据名称获取聚合
        ParsedStringTerms brandAgg = aggregations.get("brand_agg");
        // 获取聚合中的所有桶
        List<? extends Terms.Bucket> brandAggBuckets = brandAgg.getBuckets();
        for (Terms.Bucket bucket : brandAggBuckets) {
            // 获取key，也就是品牌名称，因为是按照品牌做的聚合
            String brand = bucket.getKeyAsString();
            long docCount = bucket.getDocCount();
            System.out.println(brand + "一共有 " + docCount + " 家");
        }
    }
}
