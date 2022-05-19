package com.chan.es;

import com.chan.es.dao.ProductRepository;
import com.chan.es.pojo.Product;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.query.FetchSourceFilter;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

/**
 * spring-data-elasticsearch包括 ElasticsearchTemplate 和 Repository文档操作 可以处理es需求
 *
 * @author CHAN
 * @since 2022/5/17
 */
@SpringBootTest
@RunWith(SpringRunner.class)
@Slf4j
public class SearchApplicationTest {

    // ElasticsearchTemplate一般用来处理Repository文档操作处理不了的需求
    @Resource
    ElasticsearchTemplate template;
    @Resource
    ProductRepository repository;

    /**
     * 测试创建索引和映射
     */
    @Test
    public void testCreateIndex() {
        // 创建索引 会根据Product类的@Document注解信息来创建
        template.createIndex(Product.class);

        // 创建映射 会根据Product类中的id、Field等字段来自动完成映射
        template.putMapping(Product.class);
    }

    /**
     * 删除索引的API：
     */
    @Test
    public void testDelete() {
        // 两种都可以
        // elasticsearchTemplate.deleteIndex("product");
        template.deleteIndex(Product.class);
    }


    // =================下面使用Repository进行文档操作=================

    /**
     * 测试新增文档
     */
    @Test
    public void addDoc() {
        Product product = new Product(1L, "小米手机7", " 手机", "小米", 3499.00, "http://image.leyou.com/13123.jpg");
        // save方法有增又有改 好比PUT
        repository.save(product);
    }

    /**
     * 批量新增
     */
    @Test
    public void addDocs(){
        List<Product> list = new ArrayList<>();
        list.add(new Product(2L, "坚果手机R1", " 手机", "锤子", 3699.00, "http://image.leyou.com/123.jpg"));
        list.add(new Product(3L, "华为META10", " 手机", "华为", 4499.00, "http://image.leyou.com/3.jpg"));
        list.add(new Product(4L, "Apple11Pro", "手机", "APPLE", 3299.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Product(5L, "诺基亚R1", "手机", "诺基亚", 3699.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Product(6L, "华为P50", "手机", "华为", 4499.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Product(7L, "小米Mix2S", "手机", "小米", 4299.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Product(8L, "荣耀V10", "手机", "华为", 2799.00, "http://image.leyou.com/13123.jpg"));
        // 接收对象的集合，实现批量新增
        repository.saveAll(list);
    }

    /**
     * 基本查询
     */
    @Test
    public void testFind() {
        // 查询全部，并按照价格降序排序
        Iterable<Product> products = repository.findAll(Sort.by(Sort.Direction.DESC, "price"));
        products.forEach(System.out::println);
    }

    /**
     * 自定义方法查询，Spring Data 的另一个强大功能，是根据方法名称自动实现功能。
     * 比如：你的方法名叫做：findByTitle，那么它就知道你是根据title查询，然后自动帮你完成，只需在接口中定义方法无需写实现。
     * 当然，方法名称要符合一定的约定。例如，我们来按照价格区间查询，在ProductRepository接口中定义方法findByPriceBetween
     */
    @Test
    public void queryByPriceBetween(){
        List<Product> list = repository.findByPriceBetween(2000.00, 3500.00);
        for (Product product : list) {
            System.out.println("Product = " + product);
        }
    }



    /**
     * 词条匹配查询
     */
    @Test
    public void testQuery() {
        // 词条查询
        MatchQueryBuilder queryBuilder = QueryBuilders.matchQuery("title", "小米");
        // 执行查询
        Iterable<Product> products = repository.search(queryBuilder);
        products.forEach(System.out::println);
        // 查询结果：
        // Product(id=1, title=小米手机7, category= 手机, brand=小米, price=3499.0, images=http://image.leyou.com/13123.jpg)
        // Product(id=7, title=小米Mix2S, category=手机, brand=小米, price=4299.0, images=http://image.leyou.com/13123.jpg)
    }

    /**
     * 词条匹配查询（自定义查询方式）返回的Page<Product>，默认就是分页
     */
    @Test
    public void testNativeQuery() {
        // 构建查询条件
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        // 添加基本的词条查询（match词条匹配）
        queryBuilder.withQuery(QueryBuilders.matchQuery("title", "小米"));
        // 执行搜索，获取结果
        Page<Product> productPage = repository.search(queryBuilder.build());
        // 打印总条数
        log.debug("总条数：{}", productPage.getTotalElements());
        // 打印总页数
        log.debug("总页数：{}", productPage.getTotalPages());
        productPage.forEach(System.out::println);
    }


    /**
     * 精确匹配查询（并分页）
     */
    @Test
    public void testNativeQuery2(){
        // 构建查询条件
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        // 添加基本的分词查询
        queryBuilder.withQuery(QueryBuilders.termQuery("category", "手机"));

        // 初始化分页参数
        int page = 0;
        int size = 3;
        // 设置分页参数
        queryBuilder.withPageable(PageRequest.of(page, size));

        // 执行搜索，获取结果
        Page<Product> items = repository.search(queryBuilder.build());
        // 打印总条数
        log.debug("总条数：{}", items.getTotalElements());
        // 打印总页数
        log.debug("总页数：{}", items.getTotalPages());
        // 每页大小
        log.debug("每页大小：{}", items.getSize());
        // 当前页
        log.debug("当前页：{}", items.getNumber());
        items.forEach(System.out::println);
    }

    /**
     * 查询并排序
     */
    @Test
    public void testSort(){
        // 构建查询条件
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        // 添加基本的查询（term精确匹配）
        queryBuilder.withQuery(QueryBuilders.termQuery("category", "手机"));

        // 排序
        queryBuilder.withSort(SortBuilders.fieldSort("price").order(SortOrder.DESC));

        // 执行搜索，获取结果
        Page<Product> items = repository.search(queryBuilder.build());
        // 打印总条数
        log.debug("总条数：{}", items.getTotalElements());
        items.forEach(System.out::println);
    }

    /**
     * 聚合为桶
     * 桶就是分组，比如这里我们按照品牌brand进行分组。
     * 理解：聚合就是把所有文档依据某个条件进行分组，聚合中的每个桶即分组后的每个组
     */
    @Test
    public void testAgg() {
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        // 不查询任何字段
        queryBuilder.withSourceFilter(new FetchSourceFilter(new String[]{""}, null));
        // 1.添加一个聚合，聚合的类型为terms，聚合名称设置为brands，要根据brand字段进行聚合
        queryBuilder.addAggregation(AggregationBuilders.terms("brands").field("brand"));
        // 2.查询，需要把结果强转为AggregatedPage类型
        AggregatedPage<Product> aggPage = (AggregatedPage<Product>) repository.search(queryBuilder.build());
        // 3.解析
        // 3.1从结果中取出名为brands的那个聚合
        StringTerms agg = (StringTerms) aggPage.getAggregation("brands");// brand属性是String类型的，依据brand属性进行的term聚合，所以结果要强转为StringTerm类型
        // 3.2从聚合中获取桶
        List<StringTerms.Bucket> buckets = agg.getBuckets();
        // 3.3遍历
        buckets.forEach((bucket)->{
            // 3.4获取桶中的key，即品牌名称
            System.out.println(bucket.getKeyAsString());
            // 3.5获取桶中的文档数量
            System.out.println(bucket.getDocCount());
        });
    }


    /**
     * 嵌套聚合，求平均值
     */
    @Test
    public void testSubAgg() {
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        // 不查询任何结果
        queryBuilder.withSourceFilter(new FetchSourceFilter(new String[]{""}, null));
        // 1、添加一个新的聚合，聚合类型为terms，聚合名称为brands，聚合字段为brand
        queryBuilder.addAggregation(
                AggregationBuilders.terms("brands").field("brand")
                        // 在品牌聚合桶内进行嵌套聚合，求平均值
                        .subAggregation(AggregationBuilders.avg("priceAvg").field("price"))
        );
        // 2、查询,需要把结果强转为AggregatedPage类型
        AggregatedPage<Product> aggPage = (AggregatedPage<Product>) repository.search(queryBuilder.build());
        // 3、解析
        // 3.1、从结果中取出名为brands的那个聚合，
        // 注：因为是利用String类型字段来进行的term聚合，所以结果要强转为StringTerm类型
        // StringTerms agg = (StringTerms) aggPage.getAggregation("brands"); //（或）
        StringTerms agg = (StringTerms) aggPage.getAggregations().asMap().get("brands");
        // 3.2、获取桶
        List<StringTerms.Bucket> buckets = agg.getBuckets();
        // 3.3、遍历
        for (StringTerms.Bucket bucket : buckets) {
            // 3.4、获取桶中的key，即品牌名称  3.5、获取桶中的文档数量
            System.out.println(bucket.getKeyAsString() + "，共" + bucket.getDocCount() + "台");

            // 3.6.获取子聚合结果：
            InternalAvg avg = (InternalAvg) bucket.getAggregations().asMap().get("priceAvg");
            System.out.println("平均售价：" + avg.getValue());
        }
    }
}
