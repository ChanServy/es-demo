package com.chan.es.pojo;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * sku信息
 * @TableName pms_sku_info
 */
@TableName(value ="pms_sku_info")
@Data
@Document(indexName = "skuInfo", shards = 1, replicas = 0)
public class SkuInfo implements Serializable {
    /**
     * skuId
     */
    @TableId(type = IdType.AUTO)
    @Id
    @Field(type = FieldType.Long)
    private Long skuId;

    /**
     * spuId
     */
    private Long spuId;

    /**
     * sku名称
     */
    private String skuName;

    /**
     * sku介绍描述
     */
    private String skuDesc;

    /**
     * 所属分类id
     */
    private Long catalogId;

    /**
     * 品牌id
     */
    private Long brandId;

    /**
     * 默认图片
     */
    private String skuDefaultImg;

    /**
     * 标题
     */
    @Field(type = FieldType.Text, analyzer = "ik_max_word")
    private String skuTitle;

    /**
     * 副标题
     */
    @Field(type = FieldType.Text, analyzer = "ik_max_word")
    private String skuSubtitle;

    /**
     * 价格
     */
    private BigDecimal price;

    /**
     * 销量
     */
    private Long saleCount;

    @TableField(exist = false)
    private static final long serialVersionUID = 1L;
}