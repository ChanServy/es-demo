package com.chan.es.dao;

import com.chan.es.pojo.Product;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

/**
 * @author CHAN
 * @since 2022/5/17
 */
public interface ProductRepository extends ElasticsearchRepository<Product, Long> {
    /**
     * 根据价格区间查询
     */
    List<Product> findByPriceBetween(double price1, double price2);
}
