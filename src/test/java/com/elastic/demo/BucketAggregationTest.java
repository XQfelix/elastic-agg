package com.elastic.demo;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.filter.*;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.missing.Missing;
import org.elasticsearch.search.aggregations.bucket.missing.MissingAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.ReverseNested;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTerms;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

/**
 * @Title: Bucket aggregations 桶分聚合
 * <p>
 * Bucket aggregations 不像 metrics aggregations 那样计算指标，恰恰相反，
 * 它创建文档的buckets，每个buckets与标准（取决于聚合类型）相关联，
 * 它决定了当前上下文中的文档是否会“falls”到它。换句话说，bucket可以有效地定义文档集合。
 * 除了buckets本身，bucket集合还计算并返回“落入”每个bucket的文档数量。
 * <p>
 * 与度量聚合相比，Bucket聚合可以保存子聚合，这些子聚合将针对由其“父”bucket聚合创建的bucket进行聚合。
 * <p>
 * 有不同的bucket聚合器，每个具有不同的“bucketing”策略,一些定义一个单独的bucket，
 * 一些定义多个bucket的固定数量，另一些定义在聚合过程中动态创建bucket
 */
@SpringBootTest
class BucketAggregationTest {
    private static final Logger log = LoggerFactory.getLogger(BucketAggregationTest.class);
    @Autowired
    private RestHighLevelClient highLevelClient;

    /**
     * Global Aggregation 全局聚合
     * <p>
     * 定义搜索执行上下文中的所有文档的单个bucket，这个上下文由索引和您正在搜索的文档类型定义，但不受搜索查询本身的影响。
     * 全局聚合器只能作为顶层聚合器放置，因为将全局聚合器嵌入到另一个分组聚合器中是没有意义的
     */
    @Test
    void globalAggregation() throws IOException {
        GlobalAggregationBuilder aggregation = AggregationBuilders
                .global("agg")
                .subAggregation(AggregationBuilders.terms("classCode.keyword").field("classCode.keyword"));

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(aggregation);

        SearchRequest searchRequest = new SearchRequest("dcvciclass");
        searchRequest.source(sourceBuilder);
        SearchResponse response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        Global aggResult = response.getAggregations().get("agg");
        log.info(">>> {}", aggResult.getDocCount());
    }


    /**
     * Filter Aggregation 过滤聚合
     * <p>
     * 过滤聚合——基于一个条件，来对当前的文档进行过滤的聚合。
     */
    @Test
    void filterAggregation() throws IOException {
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        query.must(QueryBuilders.termQuery("parentId", 5));

        FilterAggregationBuilder aggregation = AggregationBuilders
                .filter("agg", query);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(aggregation);
        SearchRequest searchRequest = new SearchRequest("dcvciclass");

        searchRequest.source(sourceBuilder);
        SearchResponse response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        Filter aggResult = response.getAggregations().get("agg");
        log.info(">>> {}", aggResult.getDocCount());
    }


    /**
     * Filters Aggregation 多过滤聚合
     * <p>
     * 多过滤聚合——基于多个过滤条件，来对当前文档进行【过滤】的聚合，
     * 每个过滤都包含所有满足它的文档（多个bucket中可能重复）。
     */
    @Test
    void filtersAggregation() throws IOException {
        FiltersAggregationBuilder aggregation = AggregationBuilders
                .filters("agg",
                        new FiltersAggregator.KeyedFilter("men", QueryBuilders.termQuery("parentId", 5)),
                        new FiltersAggregator.KeyedFilter("women", QueryBuilders.termQuery("parentId", 1)));

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(aggregation);
        SearchRequest searchRequest = new SearchRequest("dcvciclass");

        searchRequest.source(sourceBuilder);
        SearchResponse response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        Filters aggResult = response.getAggregations().get("agg");

        for (Filters.Bucket entry : aggResult.getBuckets()) {
            // bucket key
            String key = entry.getKeyAsString();
            // Doc count
            long docCount = entry.getDocCount();
            log.info(">>> key: {}, doc_count: {}", key, docCount);
        }
    }


    /**
     * Missing Aggregation 基于字段数据的单桶聚合
     * <p>
     * 基于字段数据的单桶聚合，创建当前文档集上下文中缺少字段值的所有文档的bucket（桶）（有效地，丢失了一个字段或配置了NULL值集），
     * 此聚合器通常与其他字段数据桶聚合器（例如范围）结合使用，以返回由于缺少字段数据值而无法放置在任何其他存储区中的所有文档的信息
     */
    @Test
    void missingAggregation() throws IOException {
        MissingAggregationBuilder aggregation = AggregationBuilders.missing("agg").field("parentId");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(aggregation);
        SearchRequest searchRequest = new SearchRequest("dcvciclass");

        searchRequest.source(sourceBuilder);
        SearchResponse response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        Missing aggResult = response.getAggregations().get("agg");
        log.info(">>> {}", aggResult.getDocCount());
    }


    /**
     * Nested Aggregation 嵌套类型聚合
     * <p>
     * 基于嵌套（nested）数据类型，把该【嵌套类型的信息】聚合到单个桶里，然后就可以对嵌套类型做进一步的聚合操作
     */
    @Test
    void nestedAggregation() throws IOException {
        NestedAggregationBuilder aggregation = AggregationBuilders.nested("agg", "parentId");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(aggregation);
        SearchRequest searchRequest = new SearchRequest("dcvciclass");

        searchRequest.source(sourceBuilder);
        SearchResponse response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        Nested aggResult = response.getAggregations().get("agg");
        log.info(">>> {}", aggResult.getDocCount());
    }


    /**
     * Reverse nested Aggregation
     * <p>
     * 一个特殊的单桶聚合，可以从嵌套文档中聚合父文档。
     * 实际上，这种聚合可以从嵌套的块结构中跳出来，并链接到其他嵌套的结构或根文档.
     * 这允许嵌套不是嵌套对象的一部分的其他聚合在嵌套聚合中。
     * reverse_nested 聚合必须定义在nested之中
     */
    @Test
    void reverseNestedAggregation() throws IOException {
        NestedAggregationBuilder aggregation = AggregationBuilders
                .nested("agg", "resellers")
                .subAggregation(
                        AggregationBuilders
                                .terms("type").field("resellers.type")
                                .subAggregation(
                                        AggregationBuilders
                                                .reverseNested("reseller_to_product")
                                )
                );

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(aggregation);
        SearchRequest searchRequest = new SearchRequest("dcvciclass");

        searchRequest.source(sourceBuilder);
        SearchResponse response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        Nested aggResult = response.getAggregations().get("agg");
        Terms name = aggResult.getAggregations().get("type");
        for (Terms.Bucket bucket : name.getBuckets()) {
            ReverseNested resellerToProduct = bucket.getAggregations().get("reseller_to_product");
            log.info(">>> {}", resellerToProduct.getDocCount());
        }
    }


    /**
     * Terms Aggregation 词元聚合
     * <p>
     * 基于某个field，该 field 内的每一个【唯一词元】为一个桶，并计算每个桶内文档个数。
     * 默认返回顺序是按照文档个数多少排序。当不返回所有 buckets 的情况，文档个数可能不准确。
     */
    @Test
    void termsAggregation() throws IOException {
        TermsAggregationBuilder aggregation = AggregationBuilders
                .terms("agg")
                .field("parentId");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(aggregation);
        SearchRequest searchRequest = new SearchRequest("dcvciclass");

        searchRequest.source(sourceBuilder);
        SearchResponse response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        Terms aggResult = response.getAggregations().get("agg");
        for (Terms.Bucket entry : aggResult.getBuckets()) {
            log.info(">>> key: {}, docCount: {}", entry.getKey(), entry.getDocCount());
        }
    }


    /**
     * Order 排序
     * <p>
     * 基于某个field，该 field 内的每一个【唯一词元】为一个桶，并计算每个桶内文档个数。
     * 默认返回顺序是按照文档个数多少排序。当不返回所有 buckets 的情况，文档个数可能不准确。
     */
    @Test
    void orderAggregation() throws IOException {
        // 通过 doc_count 按升序排列
        // TermsAggregationBuilder aggregation = AggregationBuilders
        //         .terms("agg")
        //         .field("parentId")
        //         .order(BucketOrder.count(true));

        // 通过 key 按升序排列
        // TermsAggregationBuilder aggregation = AggregationBuilders
        //         .terms("agg")
        //         .field("parentId")
        //         .order(BucketOrder.key(true));

        // 按 metrics 子聚合排列（标示为聚合名）
        TermsAggregationBuilder aggregation = AggregationBuilders
                .terms("agg")
                .field("parentId")
                .order(BucketOrder.aggregation("avg_parentId", false))
                .subAggregation(
                        AggregationBuilders.avg("avg_parentId").field("avg_parentId")
                );

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(aggregation);
        SearchRequest searchRequest = new SearchRequest("dcvciclass");

        searchRequest.source(sourceBuilder);
        SearchResponse response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        Terms aggResult = response.getAggregations().get("agg");
        for (Terms.Bucket entry : aggResult.getBuckets()) {
            log.info(">>> key: {}, docCount: {}", entry.getKey(), entry.getDocCount());
        }
    }


    /**
     * Significant Terms Aggregation
     * <p>
     * 返回集合中感兴趣的或者不常见的词条的聚合
     */
    @Test
    void significantTermsAggregation() throws IOException {
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        query.must(QueryBuilders.termQuery("parentId", 5));

        SignificantTermsAggregationBuilder aggregation = AggregationBuilders
                .significantTerms("agg")
                .field("parentId");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(aggregation);
        sourceBuilder.query(query);
        SearchRequest searchRequest = new SearchRequest("dcvciclass");

        searchRequest.source(sourceBuilder);
        SearchResponse response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SignificantTerms aggResult = response.getAggregations().get("agg");
        for (SignificantTerms.Bucket entry : aggResult.getBuckets()) {
            log.info(">>> key: {}, docCount: {}", entry.getKey(), entry.getDocCount());
        }
    }


    /**
     * Date Range Aggregation 日期范围聚合
     * <p>
     * 日期范围聚合——基于日期类型的值，以【日期范围】来桶分聚合。
     * <p>
     * 日期范围可以用各种 Date Math 表达式。
     * <p>
     * 同样的，包括 from 的值，不包括 to 的值。
     */
    @Test
    void dateRangeAggregation() throws IOException {
        AggregationBuilder aggregation =
                AggregationBuilders
                        .dateRange("agg")
                        .field("createTime")
                        .format("yyyyMMddHHmmss")
                        .addUnboundedTo("20160522161616")     // Less than 20160522161616
                        .addRange("20160522161616", "20210522161616")  // 20160522161616 --- 20210522161616
                        .addUnboundedFrom("20210522161616"); // more than 20210522161616

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(aggregation);
        SearchRequest searchRequest = new SearchRequest("dcvciclass");

        searchRequest.source(sourceBuilder);
        SearchResponse response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        Range aggResult = response.getAggregations().get("agg");

        for (Range.Bucket entry : aggResult.getBuckets()) {
            String key = entry.getKeyAsString();                // Date range as key
            DateTime fromAsDate = (DateTime) entry.getFrom();   // Date bucket from as a Date
            DateTime toAsDate = (DateTime) entry.getTo();       // Date bucket to as a Date
            long docCount = entry.getDocCount();                // Doc count
            log.info("key [{}], from [{}], to [{}], doc_count [{}]", key, fromAsDate, toAsDate, docCount);
        }
    }


    /**
     * Histogram Aggregation 直方图聚合
     * <p>
     * 基于文档中的某个【数值类型】字段，通过计算来动态的分桶。
     */
    @Test
    void histogramAggregation() throws IOException {
        AggregationBuilder aggregation = AggregationBuilders
                        .histogram("agg")
                        .field("parentId")
                        .interval(1);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(aggregation);
        SearchRequest searchRequest = new SearchRequest("dcvciclass");

        searchRequest.source(sourceBuilder);
        SearchResponse response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        Range aggResult = response.getAggregations().get("agg");

        for (Range.Bucket entry : aggResult.getBuckets()) {
            String key = entry.getKeyAsString();                // Date range as key
            DateTime fromAsDate = (DateTime) entry.getFrom();   // Date bucket from as a Date
            DateTime toAsDate = (DateTime) entry.getTo();       // Date bucket to as a Date
            long docCount = entry.getDocCount();                // Doc count
            log.info("key [{}], from [{}], to [{}], doc_count [{}]", key, fromAsDate, toAsDate, docCount);
        }
    }

}
