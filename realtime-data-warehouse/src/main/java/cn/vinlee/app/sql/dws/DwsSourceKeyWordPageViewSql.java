package cn.vinlee.app.sql.dws;

/**
 * 统计各种页面关键此出现频次Flink Sql语句.
 *
 * @author Vinlee Xiao
 * @className DwsSourceKeyWordPageViewSql
 * @date 2022/7/29 20:16:31
 **/
public class DwsSourceKeyWordPageViewSql {
    private DwsSourceKeyWordPageViewSql() {

    }

    /**
     * 页面浏览数据.
     */
    public static final String SOURCE_TOPIC_DDL = "create table page_log( " +
            "    `page` map<string,string>, " +
            "    `ts` bigint, " +
            "    `rt` as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +
            "    WATERMARK FOR rt AS rt - INTERVAL '2' SECOND " +
            ")";


    /**
     * 过滤搜索页面的Flink SQL语句.
     */
    public static final String FILTER_SEARCH_SQL = "select " +
            "    page['item'] key_word, " +
            "    rt " +
            "from " +
            "    page_log " +
            "where page['item'] is not null " +
            "and page['last_page_id'] = 'search' " +
            "and page['item_type'] = 'keyword'";


    public static final String AGEGATE_SQL = "select " +
            "    'search' source, " +
            "    DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') start_time, " +
            "    DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') end_time, " +
            "    word keyword, " +
            "    count(*) keyword_count, " +
            "    UNIX_TIMESTAMP() ts " +
            "from " +
            "    split_table " +
            "group by TUMBLE(rt, INTERVAL '10' SECOND),word";


    public static final String SINK_SQL = "create table if not exists GMALL_REALTIME.dws_traffic_source_keyword_page_view_window\n" +
            "(\n" +
            "    start_time           Time not null,\n" +
            "    end_time           Time not null,\n" +
            "    source        varchar not null,\n" +
            "    keyword       varchar not null,\n" +
            "    keyword_count bigint,\n" +
            "    ts            bigint,\n" +
            "    CONSTRAINT my_pk PRIMARY KEY (start_time,end_time,source,keyword)\n" +
            ") WITH (\n" +
            " 'connector' = 'hbase-2.4',\n" +
            " 'table-name' = 'dws_traffic_source_keyword_page_view_window',\n" +
            " 'zookeeper.quorum' = 'hadoop1,hadoop2,hadoop3:2181'\n" +
            ");";
}
