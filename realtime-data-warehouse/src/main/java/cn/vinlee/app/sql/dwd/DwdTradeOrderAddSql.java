package cn.vinlee.app.sql.dwd;

/**
 * 订单下单数据SQL.
 *
 * @author Vinlee Xiao
 * @className DwdTradeOrderDetailSql
 * @date 2022/7/25 19:44:09
 **/
public class DwdTradeOrderAddSql {
    private DwdTradeOrderAddSql() {

    }

    public static final String DWD_TRADE_ORDER_DETAIL = "create table dwd_trade_order_detail_table( " +
            "    `order_detail_id` string, " +
            "    `order_id` string, " +
            "    `sku_id` string, " +
            "    `sku_name` string, " +
            "    `order_price` string, " +
            "    `sku_num` string, " +
            "    `order_create_time` string, " +
            "    `source_type` string, " +
            "    `source_id` string, " +
            "    `split_original_amount` string, " +
            "    `split_total_amount` string, " +
            "    `split_activity_amount` string, " +
            "    `split_coupon_amount` string, " +
            "    `pt` TIMESTAMP_LTZ(3), " +
            "    `consignee` string, " +
            "    `consignee_tel` string, " +
            "    `total_amount` string, " +
            "    `order_status` string, " +
            "    `user_id` string, " +
            "    `payment_way` string, " +
            "    `out_trade_no` string, " +
            "    `trade_body` string, " +
            "    `operate_time` string, " +
            "    `expire_time` string, " +
            "    `process_status` string, " +
            "    `tracking_no` string, " +
            "    `parent_order_id` string, " +
            "    `province_id` string, " +
            "    `activity_reduce_amount` string, " +
            "    `coupon_reduce_amount` string, " +
            "    `original_total_amount` string, " +
            "    `feight_fee` string, " +
            "    `feight_fee_reduce` string, " +
            "    `type` string, " +
            "    `old` map<string,string>, " +
            "    `activity_id` string, " +
            "    `activity_rule_id` string, " +
            "    `activity_create_time` string , " +
            "    `coupon_id` string, " +
            "    `coupon_use_id` string, " +
            "    `coupon_create_time` string , " +
            "    `dic_name` string " +
            ")";

    public static final String SINK_TOPIC_DDL = "create table dwd_trade_order_add_table( " +
            "    `order_detail_id` string, " +
            "    `order_id` string, " +
            "    `sku_id` string, " +
            "    `sku_name` string, " +
            "    `order_price` string, " +
            "    `sku_num` string, " +
            "    `order_create_time` string, " +
            "    `source_type` string, " +
            "    `source_id` string, " +
            "    `split_original_amount` string, " +
            "    `split_total_amount` string, " +
            "    `split_activity_amount` string, " +
            "    `split_coupon_amount` string, " +
            "    `pt` TIMESTAMP_LTZ(3), " +
            "    `consignee` string, " +
            "    `consignee_tel` string, " +
            "    `total_amount` string, " +
            "    `order_status` string, " +
            "    `user_id` string, " +
            "    `payment_way` string, " +
            "    `out_trade_no` string, " +
            "    `trade_body` string, " +
            "    `operate_time` string, " +
            "    `expire_time` string, " +
            "    `process_status` string, " +
            "    `tracking_no` string, " +
            "    `parent_order_id` string, " +
            "    `province_id` string, " +
            "    `activity_reduce_amount` string, " +
            "    `coupon_reduce_amount` string, " +
            "    `original_total_amount` string, " +
            "    `feight_fee` string, " +
            "    `feight_fee_reduce` string, " +
            "    `type` string, " +
            "    `old` map<string,string>, " +
            "    `activity_id` string, " +
            "    `activity_rule_id` string, " +
            "    `activity_create_time` string , " +
            "    `coupon_id` string, " +
            "    `coupon_use_id` string, " +
            "    `coupon_create_time` string , " +
            "    `dic_name` string " +
            ")";


}
