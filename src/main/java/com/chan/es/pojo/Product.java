package com.chan.es.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

/**
 * @author CHAN
 * @since 2022/5/17
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Document(indexName = "product", shards = 1, replicas = 0)
public class Product {
    @Id
    @Field(type = FieldType.Long)
    private Long id;// 如果id不指定，es会随机生成一个文档id

    @Field(type = FieldType.Text, analyzer = "ik_max_word")
    private String title; //标题

    @Field(type = FieldType.Keyword)
    private String category;// 分类

    @Field(type = FieldType.Keyword)
    private String brand; // 品牌

    @Field(type = FieldType.Double)
    private Double price; // 价格

    @Field(type = FieldType.Keyword, index = false)
    private String images; // 图片地址
}
