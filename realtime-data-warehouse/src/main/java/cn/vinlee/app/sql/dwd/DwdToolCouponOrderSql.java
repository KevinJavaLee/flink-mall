package cn.vinlee.app.sql.dwd;

/**
 * @author Vinlee Xiao
 * @className DwdToolCouponOrderSql
 * @date 2022/7/26 19:51:59
 **/
public class DwdToolCouponOrderSql {

    private DwdToolCouponOrderSql() {

    }


    /**
     * 源数据.
     */
    public static final String SOURCE_TOPIC = "create table `ods_base_db`( " +
            "`database` string, " +
            "`table` string, " +
            "`data` map<string, string>, " +
            "`type` string, " +
            "`old` string, " +
            "`ts` string " +
            ")";

    /**
     * 优惠券领取数据.
     */
    public static final String COUPON_USE = "select " +
            "data['id'] id, " +
            "data['coupon_id'] couponId, " +
            "data['user_id'] userId, " +
            "data['order_id'] orderId, " +
            "date_format(data['using_time'],'yyyy-MM-dd') dateId, " +
            "data['using_time'] usingTime, " +
            "`old`, " +
            "ts " +
            "from ods_base_db " +
            "where `table` = 'coupon_use' " +
            "and `type` = 'update' ";


    /**
     * 保存到Kafka中的数据.
     */
    public static final String SINK_TOPIC = "create table dwd_tool_coupon_order_detail( " +
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
