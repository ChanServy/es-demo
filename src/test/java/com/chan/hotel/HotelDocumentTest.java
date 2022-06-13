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
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;

/**
 *
 * 文档操作的基本步骤：
 *
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
        this.client = new RestHighLevelClient(RestClient.builder(HttpHost.create("http://127.0.0.1:9200")));
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
                request.add(new IndexRequest("hotel")
                        .id(hotelDoc.getId().toString())
                        .source(JSONUtil.toJsonStr(hotelDoc), XContentType.JSON));
            });
        }
        // 3.发送请求
        client.bulk(request, RequestOptions.DEFAULT);
    }

}
