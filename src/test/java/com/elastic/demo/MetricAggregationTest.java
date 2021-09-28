package com.elastic.demo;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentilesAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.StatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * @Title: 计算度量聚合
 * <p>
 * 计算度量这类的聚合操作是以使用一种方式或者从文档中提取需要聚合的值为基础的。
 * 这些数据不但可以从文档（使用数据属性）的属性中提取出来，也可以使用脚本生成。
 * <p>
 * 数值计量聚合操作是能够产生具体的数值的一种计量聚合操作。
 * 一些聚合操作输出单个的计量数值（例如avg），并且被称作single-value numeric metric aggregation，
 * 其他产生多个计量数值（例如 stats）的称作 multi-value numeric metrics aggregation。
 * 这两种不同的聚合操作只有在桶聚合的子聚合操作中才会有不同的表现（有些桶聚合可以基于每个的数值计量来对返回的桶进行排序）
 */
@SpringBootTest
class MetricAggregationTest {
    private static final Logger log = LoggerFactory.getLogger(MetricAggregationTest.class);
    @Autowired
    private RestHighLevelClient highLevelClient;

    /**
     * Min Aggregatione 最小值聚合
     */
    @Test
    void metricsMin() throws IOException {
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        query.must(QueryBuilders.rangeQuery("classLvl").gte(2));

        MinAggregationBuilder aggregation =
                AggregationBuilders
                        .min("agg")
                        .field("classLvl");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(query);
        sourceBuilder.aggregation(aggregation);

        SearchRequest searchRequest = new SearchRequest("dcvciclass");
        searchRequest.source(sourceBuilder);
        SearchResponse response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        Min aggResult = response.getAggregations().get("agg");
        log.info(">>> {}", BigDecimal.valueOf(aggResult.getValue()));
    }

    /**
     * Max Aggregation 最大值聚合
     */
    @Test
    void metricsMax() throws IOException {
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        query.must(QueryBuilders.rangeQuery("classLvl").gte(2));

        MaxAggregationBuilder aggregation =
                AggregationBuilders
                        .max("agg")
                        .field("classLvl");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(query);
        sourceBuilder.aggregation(aggregation);

        SearchRequest searchRequest = new SearchRequest("dcvciclass");
        searchRequest.source(sourceBuilder);
        SearchResponse response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        Max aggResult = response.getAggregations().get("agg");
        log.info(">>> {}", BigDecimal.valueOf(aggResult.getValue()));
    }


    /**
     * Sum Aggregation 求和聚合
     */
    @Test
    void metricsSum() throws IOException {
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        query.must(QueryBuilders.rangeQuery("classLvl").gte(2));

        SumAggregationBuilder aggregation =
                AggregationBuilders
                        .sum("agg")
                        .field("classLvl");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(query);
        sourceBuilder.aggregation(aggregation);

        SearchRequest searchRequest = new SearchRequest("dcvciclass");
        searchRequest.source(sourceBuilder);
        SearchResponse response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        Sum aggResult = response.getAggregations().get("agg");
        log.info(">>> {}", BigDecimal.valueOf(aggResult.getValue()));
    }


    /**
     * Avg Aggregation 平均值聚合
     */
    @Test
    void metricsAvg() throws IOException {
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        query.must(QueryBuilders.rangeQuery("classLvl").gte(2));

        AvgAggregationBuilder aggregation =
                AggregationBuilders
                        .avg("agg")
                        .field("classLvl");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(query);
        sourceBuilder.aggregation(aggregation);

        SearchRequest searchRequest = new SearchRequest("dcvciclass");
        searchRequest.source(sourceBuilder);
        SearchResponse response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        Avg aggResult = response.getAggregations().get("agg");
        log.info(">>> {}", BigDecimal.valueOf(aggResult.getValue()));
    }


    /**
     * Stats Aggregation 统计聚合
     * <p>
     * 统计聚合——基于文档的某个值，计算出一些统计信息（min、max、sum、count、avg）,
     * 用于计算的值可以是特定的数值型字段，也可以通过脚本计算而来。
     */
    @Test
    void metricsStats() throws IOException {
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        query.must(QueryBuilders.rangeQuery("classLvl").gte(2));

        StatsAggregationBuilder aggregation =
                AggregationBuilders
                        .stats("agg")
                        .field("classLvl");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(query);
        sourceBuilder.aggregation(aggregation);

        SearchRequest searchRequest = new SearchRequest("dcvciclass");
        searchRequest.source(sourceBuilder);
        SearchResponse response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        Stats aggResult = response.getAggregations().get("agg");

        log.info("Min >>> {}", BigDecimal.valueOf(aggResult.getMin()));
        log.info("Max >>> {}", BigDecimal.valueOf(aggResult.getMax()));
        log.info("Avg >>> {}", BigDecimal.valueOf(aggResult.getAvg()));
        log.info("Sum >>> {}", BigDecimal.valueOf(aggResult.getSum()));
        log.info("Count >>> {}", aggResult.getCount());
    }


    /**
     * Extended Stats Aggregation 扩展统计聚合
     * <p>
     * 扩展统计聚合——基于文档的某个值，计算出一些统计信息
     * （比普通的stats聚合多了sum_of_squares、variance、std_deviation、std_deviation_bounds）,
     * 用于计算的值可以是特定的数值型字段，也可以通过脚本计算而来。
     */
    @Test
    void metricsExtendedStats() throws IOException {
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        query.must(QueryBuilders.rangeQuery("classLvl").gte(20));

        ExtendedStatsAggregationBuilder aggregation =
                AggregationBuilders
                        .extendedStats("agg")
                        .field("classLvl");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(query);
        sourceBuilder.aggregation(aggregation);

        SearchRequest searchRequest = new SearchRequest("dcvciclass");
        searchRequest.source(sourceBuilder);
        SearchResponse response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        ExtendedStats aggResult = response.getAggregations().get("agg");

        log.info("Min >>> {}", BigDecimal.valueOf(aggResult.getMin()));
        log.info("Max >>> {}", BigDecimal.valueOf(aggResult.getMax()));
        log.info("Avg >>> {}", BigDecimal.valueOf(aggResult.getAvg()));
        log.info("Sum >>> {}", BigDecimal.valueOf(aggResult.getSum()));
        log.info("Count >>> {}", aggResult.getCount());

        log.info("stdDeviation >>> {}", BigDecimal.valueOf(aggResult.getStdDeviation()));
        log.info("sumOfSquares >>> {}", BigDecimal.valueOf(aggResult.getSumOfSquares()));
        log.info("sumOfSquares >>> {}", BigDecimal.valueOf(aggResult.getVariance()));
    }


    /**
     * Value Count Aggregation 值计数聚合
     * <p>
     * 值计数聚合——计算聚合文档中某个值的个数, 用于计算的值可以是特定的数值型字段，也可以通过脚本计算而来。
     */
    @Test
    void metricsValueCount() throws IOException {
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        query.must(QueryBuilders.rangeQuery("classLvl").gte(20));

        ValueCountAggregationBuilder aggregation =
                AggregationBuilders
                        .count("agg")
                        .field("classLvl");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(query);
        sourceBuilder.aggregation(aggregation);

        SearchRequest searchRequest = new SearchRequest("dcvciclass");
        searchRequest.source(sourceBuilder);
        SearchResponse response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        ValueCount aggResult = response.getAggregations().get("agg");
        log.info("Count >>> {}", BigDecimal.valueOf(aggResult.getValue()));
    }


    /**
     * Percentile Aggregation 百分百聚合
     * <p>
     * 百分百聚合——基于聚合文档中某个数值类型的值，求这些值中的一个或者多个百分比,
     * 用于计算的值可以是特定的数值型字段，也可以通过脚本计算而来。
     */
    @Test
    void metricsPercentiles() throws IOException {
        PercentilesAggregationBuilder aggregation =
                AggregationBuilders
                        .percentiles("agg")
                        .field("id");
        // 自定义百分数位
        // .percentiles(1.0, 6.0, 10.0, 20.0, 30.0, 75.0, 95.0, 99.0);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(aggregation);

        SearchRequest searchRequest = new SearchRequest("dcvciclass");
        searchRequest.source(sourceBuilder);
        SearchResponse response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        Percentiles aggResult = response.getAggregations().get("agg");
        aggResult.forEach(result -> {
            log.info("percent >>> {}", result.getPercent());
            log.info("value >>> {}", result.getValue());
        });
    }


    /**
     * Percentile Ranks Aggregation 百分比等级聚合
     * <p>
     * 一个multi-value指标聚合，它通过从聚合文档中提取数值来计算一个或多个百分比。
     */
    @Test
    void metricsPercentilesRanks() throws IOException {
        PercentilesAggregationBuilder aggregation =
                AggregationBuilders
                        .percentiles("agg")
                        .field("classLvl");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(aggregation);

        SearchRequest searchRequest = new SearchRequest("dcvciclass");
        searchRequest.source(sourceBuilder);
        SearchResponse response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        PercentileRanks aggResult = response.getAggregations().get("agg");
        aggResult.forEach(result -> {
            log.info("percent >>> {}", result.getPercent());
            log.info("value >>> {}", result.getValue());
        });
    }


    /**
     * Top Hits Aggregation 最高匹配权值聚合
     * <p>
     * 最高匹配权值聚合——跟踪聚合中相关性最高的文档。
     * 该聚合一般用做 sub-aggregation，以此来聚合每个桶中的最高匹配的文档。
     */
    @Test
    void metricsTopHits() throws IOException {
        // 大多数标准的搜索选项可以使用 from, size, sort, highlight, explain 等
        AggregationBuilder aggregation = AggregationBuilders
                .terms("agg").field("parentId")
                .subAggregation(
                        AggregationBuilders.topHits("top")
                                // .explain(true)
                                .size(1)
                                .from(10)
                );

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.boolQuery());
        sourceBuilder.aggregation(aggregation);

        SearchRequest searchRequest = new SearchRequest("dcvciclass");
        searchRequest.source(sourceBuilder);
        SearchResponse response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        Terms aggResult = response.getAggregations().get("agg");

        for (Terms.Bucket entry : aggResult.getBuckets()) {
            log.info(">>> bucket_key: {}, doc_count: {}", entry.getKey(), entry.getDocCount());

            // We ask for top_hits for each bucket
            TopHits topHits = entry.getAggregations().get("top");
            for (SearchHit hit : topHits.getHits().getHits()) {
                log.info(">>> id [{}], _source [{}]", hit.getId(), hit.getSourceAsString());
            }
        }
    }
}
