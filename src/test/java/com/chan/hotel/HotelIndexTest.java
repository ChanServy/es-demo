package com.chan.hotel;

import com.chan.hotel.constants.HotelConstant;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 *
 * JavaRestClient操作elasticsearch的流程基本类似。核心是client.indices()方法来获取索引库的操作对象。
 *
 * 索引库操作的基本步骤：
 * 1.初始化RestHighLevelClient
 * 2.创建XxxIndexRequest对象。XXX是Create、Get、Delete
 * 3.准备DSL（ Create时需要，其它是无参）
 * 4.发送请求。调用RestHighLevelClient#.indices().xxx()方法，xxx是create、exists、delete
 *
 *
 *
 * @author CHAN
 * @since 2022/6/13
 */
public class HotelIndexTest {

    private RestHighLevelClient client;

    /**
     * 初始化RestHighLevelClient
     */
    @BeforeEach
    void setUp() {
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://127.0.0.1:9200")
        ));
    }

    @AfterEach
    void tearDown() throws IOException {
        this.client.close();
    }

    /**
     * 在es中创建hotel索引
     */
    @Test
    void createHotelIndex() throws IOException {
        // 1.创建Request对象
        CreateIndexRequest request = new CreateIndexRequest("hotel");
        // 2.准备请求的参数：DSL语句
        request.source(HotelConstant.MAPPING_TEMPLATE, XContentType.JSON);
        // 3.发送请求，client.indices()方法的返回值是IndicesClient类型，封装了所有与索引操作有关的方法
        client.indices().create(request, RequestOptions.DEFAULT);
    }

    /**
     * 删除hotel索引
     */
    @Test
    void testDeleteHotelIndex() throws IOException {
        // 1.创建Request对象
        DeleteIndexRequest request = new DeleteIndexRequest("hotel");
        // 2.发送请求
        client.indices().delete(request, RequestOptions.DEFAULT);
    }


    /**
     * 判断hotel索引库是否存在
     */
    @Test
    void testExistsHotelIndex() throws IOException {
        // 1.创建Request对象
        GetIndexRequest request = new GetIndexRequest("hotel");
        // 2.发送请求
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        // 3.输出
        System.err.println(exists ? "索引库已经存在！" : "索引库不存在！");
    }
}
