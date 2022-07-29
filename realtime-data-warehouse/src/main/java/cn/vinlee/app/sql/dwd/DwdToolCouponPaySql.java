package cn.vinlee.app.sql.dwd;

/**
 * @author Vinlee Xiao
 * @className DwdToolCouponPaySql
 * @date 2022/7/26 20:43:48
 **/
public class DwdToolCouponPaySql {

    private DwdToolCouponPaySql() {

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
     * 优惠券使用数据.
     */
    public static final String COUPON_USE = "select " +
            "data['id'] id, " +
            "data['coupon_id'] couponId, " +
            "data['user_id'] userId, " +
            "data['order_id'] orderId, " +
            "date_format(data['used_time'],'yyyy-MM-dd') dateId, " +
            "data['used_time'] usedTime, " +
            "`old`, " +
            "ts " +
            "from ods_base_db " +
            "where `table` = 'coupon_use' " +
            "and `type` = 'update' ";


    public static final String SINK_TOPIC = "create table dwd_tool_coupon_pay_detail( " +
            "id string, " +
            "coupon_id string, " +
            "user_id string, " +
            "order_id string, " +
            "date_id string, " +
            "payment_time string, " +
            "ts string, " +
            "primary key(id) not enforced " +
            ")";

}
