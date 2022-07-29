package cn.vinlee.app.sql.dwd;

/**
 * 工具优惠券领取事实表.
 *
 * @author Vinlee Xiao
 * @className DwdToolCouponGetSql
 * @date 2022/7/26 19:24:44
 **/
public class DwdToolCouponGetSql {

    private DwdToolCouponGetSql() {

    }

    /**
     * 源数据.
     */
    public static final String SOURCE_TOPIC = "create table `ods_base_db`( " +
            "`database` string, " +
            "`table` string, " +
            "`data` map<string, string>, " +
            "`type` string, " +
            "`ts` string " +
            ")";


    /**
     * 优惠券领取数据.
     */
    public static final String COUPON_USE = "select " +
            "data['id'], " +
            "data['coupon_id'], " +
            "data['user_id'], " +
            "date_format(data['get_time'],'yyyy-MM-dd') date_id, " +
            "data['get_time'], " +
            "ts " +
            "from ods_base_db " +
            "where `table` = 'coupon_use' " +
            "and `type` = 'insert' ";


    /**
     * 保存到Kafka对应的主题语句
     */
    public static final String DWD_TOOL_COUPON_GET = "create table dwd_tool_coupon_get ( " +
            "id string, " +
            "coupon_id string, " +
            "user_id string, " +
            "date_id string, " +
            "get_time string, " +
            "ts string, " +
            "primary key(id) not enforced " +
            ")";

    /**
     * 保存到Kafka中的主题.
     */
    public static final String SINK_TOPIC = "create table dwd_tool_coupon_order( " +
            "id string, " +
            "coupon_id string, " +
            "user_id string, " +
            "order_id string, " +
            "date_id string, " +
            "order_time string, " +
            "ts string, " +
            "primary key(id) not enforced " +
            ")";
}
