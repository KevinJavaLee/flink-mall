package cn.vinlee.app.sql.dwd;

/**
 * @author Vinlee Xiao
 * @className TradeOrderDetailSql
 * @date 2022/7/23 17:48:08
 **/
public class TradeOrderDetailSql {

    private TradeOrderDetailSql() {

    }

    /**
     * 订单明细数据.
     */
    public static final String ORDER_DETAIL = "select \n" +
            "    data['id'] order_detail_id, \n" +
            "    data['order_id'] order_id, \n" +
            "    data['sku_id'] sku_id, \n" +
            "    data['sku_name'] sku_name, \n" +
            "    data['order_price'] order_price, \n" +
            "    data['sku_num'] sku_num, \n" +
            "    data['create_time'] create_time, \n" +
            "    data['source_type'] source_type, \n" +
            "    data['source_id'] source_id, \n" +
            "    cast(cast(data['sku_num'] as decimal(16,2)) * cast(data['order_price'] as decimal(16,2)) as String) split_original_amount, \n" +
            "    data['split_total_amount'] split_total_amount, \n" +
            "    data['split_activity_amount'] split_activity_amount, \n" +
            "    data['split_coupon_amount'] split_coupon_amount, \n" +
            "    pt \n" +
            "from ods_base_db \n" +
            "where `database`='gmall' \n" +
            "and `table`='order_detail' \n" +
            "and `type`='insert' \n";

    /**
     * 订单信息.
     */
    public static final String ORDER_INFO = "select " +
            "    data['id'] id, \n" +
            "    data['consignee'] consignee, \n" +
            "    data['consignee_tel'] consignee_tel, \n" +
            "    data['total_amount'] total_amount, \n" +
            "    data['order_status'] order_status, \n" +
            "    data['user_id'] user_id, \n" +
            "    data['payment_way'] payment_way, \n" +
            "    data['out_trade_no'] out_trade_no, \n" +
            "    data['trade_body'] trade_body, \n" +
            "    data['operate_time'] operate_time, \n" +
            "    data['expire_time'] expire_time, \n" +
            "    data['process_status'] process_status, \n" +
            "    data['tracking_no'] tracking_no, \n" +
            "    data['parent_order_id'] parent_order_id, \n" +
            "    data['province_id'] province_id, \n" +
            "    data['activity_reduce_amount'] activity_reduce_amount, \n" +
            "    data['coupon_reduce_amount'] coupon_reduce_amount, \n" +
            "    data['original_total_amount'] original_total_amount, \n" +
            "    data['feight_fee'] feight_fee, \n" +
            "    data['feight_fee_reduce'] feight_fee_reduce, \n" +
            "    `type`, \n" +
            "    `old` " +
            "from ods_base_db " +
            "where `database`='gmall' " +
            "and `table`='order_info' " +
            "and (`type`='insert' or `type`='update')";


    /**
     * 订单明细活动数据.
     */
    public static final String ORDER_DETAIL_ACTIVITY = "select " +
            "    data['id'] id, \n" +
            "    data['order_id'] order_id, \n" +
            "    data['order_detail_id'] order_detail_id, \n" +
            "    data['activity_id'] activity_id, \n" +
            "    data['activity_rule_id'] activity_rule_id, \n" +
            "    data['sku_id'] sku_id, \n" +
            "    data['create_time'] create_time " +
            "from ods_base_db " +
            "where `database`='gmall' " +
            "and `table`='order_detail_activity' " +
            "and `type`='insert'";

    /**
     * 订单明细购物券数据.
     */
    public static final String ORDER_DETAIL_COUPON = "select " +
            "    data['id'] id, \n" +
            "    data['order_id'] order_id, \n" +
            "    data['order_detail_id'] order_detail_id, \n" +
            "    data['coupon_id'] coupon_id, \n" +
            "    data['coupon_use_id'] coupon_use_id, \n" +
            "    data['sku_id'] sku_id, \n" +
            "    data['create_time'] create_time " +
            "from ods_base_db " +
            "where `database`='gmall' " +
            "and `table`='order_detail_coupon' " +
            "and `type`='insert'";

    /**
     * 5表连接语句.
     */
    public static final String ORDER_DETAIL_JOIN = "select " +
            "    od.order_detail_id, \n" +
            "    od.order_id, \n" +
            "    od.sku_id, \n" +
            "    od.sku_name, \n" +
            "    od.order_price, \n" +
            "    od.sku_num, \n" +
            "    od.create_time order_create_time, \n" +
            "    od.source_type, \n" +
            "    od.source_id, \n" +
            "    od.split_original_amount, \n" +
            "    od.split_total_amount, \n" +
            "    od.split_activity_amount, \n" +
            "    od.split_coupon_amount, \n" +
            "    od.pt, \n" +
            "    oi.consignee, \n" +
            "    oi.consignee_tel, \n" +
            "    oi.total_amount, \n" +
            "    oi.order_status, \n" +
            "    oi.user_id, \n" +
            "    oi.payment_way, \n" +
            "    oi.out_trade_no, \n" +
            "    oi.trade_body, \n" +
            "    oi.operate_time, \n" +
            "    oi.expire_time, \n" +
            "    oi.process_status, \n" +
            "    oi.tracking_no, \n" +
            "    oi.parent_order_id, \n" +
            "    oi.province_id, \n" +
            "    oi.activity_reduce_amount, \n" +
            "    oi.coupon_reduce_amount, \n" +
            "    oi.original_total_amount, \n" +
            "    oi.feight_fee, \n" +
            "    oi.feight_fee_reduce, \n" +
            "    oi.`type`, \n" +
            "    oi.`old`, \n" +
            "    oa.activity_id, \n" +
            "    oa.activity_rule_id, \n" +
            "    oa.create_time activity_create_time, \n" +
            "    oc.coupon_id, \n" +
            "    oc.coupon_use_id, \n" +
            "    oc.create_time coupon_create_time, \n" +
            "    dic.dic_name, \n" +
            "    current_row_timestamp() ts " +
            "from order_detail od " +
            "join order_info oi " +
            "on od.order_id = oi.id " +
            "left join order_activity oa " +
            "on od.order_detail_id = oa.order_detail_id " +
            "left join order_coupon oc " +
            "on od.order_detail_id = oc.order_detail_id " +
            "join base_dic FOR SYSTEM_TIME AS OF od.pt as dic " +
            "on od.source_type = dic.dic_code";


    public static final String SINK_TOPIC_SQL = "" +
            "create table dwd_trade_order_detail_table( " +
            "    `order_detail_id` string, \n " +
            "    `order_id` string, \n " +
            "    `sku_id` string, \n " +
            "    `sku_name` string, \n " +
            "    `order_price` string, \n " +
            "    `sku_num` string, \n " +
            "    `order_create_time` string, \n " +
            "    `source_type` string, \n " +
            "    `source_id` string, \n " +
            "    `split_original_amount` string, \n " +
            "    `split_total_amount` string, \n " +
            "    `split_activity_amount` string, \n " +
            "    `split_coupon_amount` string, \n " +
            "    `pt` TIMESTAMP_LTZ(3), \n " +
            "    `consignee` string, \n " +
            "    `consignee_tel` string, \n " +
            "    `total_amount` string, \n " +
            "    `order_status` string, \n " +
            "    `user_id` string, \n " +
            "    `payment_way` string, \n " +
            "    `out_trade_no` string, \n " +
            "    `trade_body` string, \n " +
            "    `operate_time` string, \n " +
            "    `expire_time` string, \n " +
            "    `process_status` string, \n " +
            "    `tracking_no` string, \n " +
            "    `parent_order_id` string, \n " +
            "    `province_id` string, \n " +
            "    `activity_reduce_amount` string, \n " +
            "    `coupon_reduce_amount` string, \n " +
            "    `original_total_amount` string, \n " +
            "    `feight_fee` string, \n " +
            "    `feight_fee_reduce` string, \n " +
            "    `type` string, \n " +
            "    `old` map<string, \nstring>, \n " +
            "    `activity_id` string, \n " +
            "    `activity_rule_id` string, \n " +
            "    `activity_create_time` string , \n " +
            "    `coupon_id` string, \n " +
            "    `coupon_use_id` string, \n " +
            "    `coupon_create_time` string , \n " +
            "    `dic_name` string, \n " +
            "    `ts` TIMESTAMP_LTZ(3), \n " +
            "    PRIMARY KEY (order_detail_id) NOT ENFORCED " +
            ")";
}
