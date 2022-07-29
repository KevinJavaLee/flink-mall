package cn.vinlee.app.sql.dwd;

/**
 * 评论明细表.
 *
 * @author Vinlee Xiao
 * @className DwdInteractionCommentSql
 * @date 2022/7/26 21:53:42
 **/
public class DwdInteractionCommentSql {
    private DwdInteractionCommentSql() {

    }

    /**
     * 数据源.
     */
    public static final String SOURCE_TOPIC = "create table ods_base_db(" +
            "`database` string, " +
            "`table` string, " +
            "`type` string, " +
            "`data` map<string, string>, " +
            "`proc_time` as PROCTIME(), " +
            "`ts` string " +
            ")";


    /**
     * 评论信息数据表.
     */
    public static final String COMMENT_INFO = "select " +
            "data['id'] id, " +
            "data['user_id'] user_id, " +
            "data['sku_id'] sku_id, " +
            "data['order_id'] order_id, " +
            "data['create_time'] create_time, " +
            "data['appraise'] appraise, " +
            "proc_time, " +
            "ts " +
            "from ods_base_db " +
            "where `table` = 'comment_info' " +
            "and `type` = 'insert' ";


    /**
     * 三表连接.
     */
    public static final String JOIN_TABLE_DDL = "select " +
            "ci.id, " +
            "ci.user_id, " +
            "ci.sku_id, " +
            "ci.order_id, " +
            "date_format(ci.create_time,'yyyy-MM-dd') date_id, " +
            "ci.create_time, " +
            "ci.appraise, " +
            "dic.dic_name, " +
            "ts " +
            "from comment_info ci " +
            "left join " +
            "base_dic for system_time as of ci.proc_time as dic " +
            "on ci.appraise = dic.dic_code";


    /**
     * 保存到Kafka中主题的Flink sql.
     */
    public static final String SINK_TOPIC = "create table dwd_interaction_comment_detail( " +
            "id string, " +
            "user_id string, " +
            "sku_id string, " +
            "order_id string, " +
            "date_id string, " +
            "create_time string, " +
            "appraise_code string, " +
            "appraise_name string, " +
            "ts string, " +
            "primary key(id) not enforced " +
            ")";

}
