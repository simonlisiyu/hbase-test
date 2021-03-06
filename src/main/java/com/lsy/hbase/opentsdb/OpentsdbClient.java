package com.lsy.hbase.opentsdb;

/**
 * Created by lisiyu on 2017/3/1.
 */
public class OpentsdbClient {

    /**
     * tagv的过滤规则: 精确匹配多项迭代值，多项迭代值以'|'分隔，大小写敏感
     */
    public static String FILTER_TYPE_LITERAL_OR = "literal_or";

    /**
     * tagv的过滤规则: 通配符匹配，大小写敏感
     */
    public static String FILTER_TYPE_WILDCARD = "wildcard";

    /**
     * tagv的过滤规则: 正则表达式匹配
     */
    public static String FILTER_TYPE_REGEXP = "regexp";


    /**
     * tagv的过滤规则: 精确匹配多项迭代值，多项迭代值以'|'分隔，忽略大小写
     */
    public static String FILTER_TYPE_ILITERAL_OR = "iliteral_or";


    /**
     * tagv的过滤规则: 通配符匹配，忽略大小写
     */
    public static String FILTER_TYPE_IWILDCARD = "iwildcard";


    /**
     * tagv的过滤规则: 通配符取非匹配，大小写敏感
     */
    public static String FILTER_TYPE_NOT_LITERAL_OR = "not_literal_or";

    /**
     * tagv的过滤规则: 通配符取非匹配，忽略大小写
     */
    public static String FILTER_TYPE_NOT_ILITERAL_OR = "not_iliteral_or";


    /**
     * tagv的过滤规则:
     * <p/>
     * Skips any time series with the given tag key, regardless of the value.
     * This can be useful for situations where a metric has inconsistent tag sets.
     * NOTE: The filter value must be null or an empty string
     */
    public static String FILTER_TYPE_NOT_KEY = "not_key";


    /**
     * 查询数据，返回的数据为json格式。
     *
     * @param metric     要查询的指标
     * @param filter     查询过滤的条件, 原来使用的tags在v2.2后已不适用
     *                   filter.setType(): 设置过滤类型, 如: wildcard, regexp
     *                   filter.setTagk(): 设置tag
     *                   filter.setFilter(): 根据type设置tagv的过滤表达式, 如: hqdApp|hqdWechat
     *                   filter.setGroupBy():设置成true, 不设置或设置成false会导致读超时
     * @param aggregator 查询的聚合类型, 如: OpentsdbClient.AGGREGATOR_AVG, OpentsdbClient.AGGREGATOR_SUM
     * @param downsample 采样的时间粒度, 如: 1s,2m,1h,1d,2d
     * @param startTime  查询开始时间,时间格式为yyyy-MM-dd HH:mm:ss
     * @param endTime    查询结束时间,时间格式为yyyy-MM-dd HH:mm:ss
     */

    /**
     * 查询数据，返回的数据为json格式，结构为：
     * "[
     * "  {
     * "    metric: mysql.innodb.row_lock_time,
     * "    tags: {
     * "      host: web01,
     * "      dc: beijing
     * "    },
     * "    aggregateTags: [],
     * "    dps: {
     * "      1435716527: 1234,
     * "      1435716529: 2345
     * "    }
     * "  },
     * "  {
     * "    metric: mysql.innodb.row_lock_time,
     * "    tags: {
     * "      host: web02,
     * "      dc: beijing
     * "    },
     * "    aggregateTags: [],
     * "    dps: {
     * "      1435716627: 3456
     * "    }
     * "  }
     * "]";
     *
     * @param metric     要查询的指标
     * @param tagk       tagk
     * @param tagvFtype  tagv的过滤规则
     * @param tagvFilter tagv的匹配字符
     * @param aggregator 查询的聚合类型, 如: OpentsdbClient.AGGREGATOR_AVG, OpentsdbClient.AGGREGATOR_SUM
     * @param downsample 采样的时间粒度, 如: 1s,2m,1h,1d,2d
     * @param startTime  查询开始时间,时间格式为yyyy-MM-dd HH:mm:ss
     * @param endTime    查询结束时间,时间格式为yyyy-MM-dd HH:mm:ss
     * @return
     */



}
