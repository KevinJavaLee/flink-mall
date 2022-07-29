package cn.vinlee.app.sql.dwd;

/**
 * @author Vinlee Xiao
 * @className DwdTradeOrderRefundSql
 * @date 2022/7/25 22:02:16
 **/
public class DwdTradeOrderRefundSql {
    private DwdTradeOrderRefundSql() {

    }

    /**
     * 业务数据.
     */
    public static final String ODS_BASE_DB = "create table ods_base_db(" +
            "`database` string, " +
            "`table` string, " +
            "`type` string, " +
            "`data` map<string, string>, " +
            "`old` string, " +
            "`proc_time` as PROCTIME(), " +
            "`ts` string " +
            ")";


    /**
     * 退单表信息表.
     */
    public static final String ORDER_REFUND_TABLE = "select " +
            "data['id'] id, " +
            "data['user_id'] user_id, " +
            "data['order_id'] order_id, " +
            "data['sku_id'] sku_id, " +
            "data['refund_type'] refund_type, " +
            "data['refund_num'] refund_num, " +
            "data['refund_amount'] refund_amount, " +
            "data['refund_reason_type'] refund_reason_type, " +
            "data['refund_reason_txt'] refund_reason_txt, " +
            "data['create_time'] create_time, " +
            "proc_time, " +
            "ts " +
            "from ods_base_db " +
            "where `table` = 'order_refund_info' " +
            "and `type` = 'insert' ";


    /**
     * 订单信息.
     */
    public static final String ORDER_INFO_TABLE = "select " +
            "data['id'] id, " +
            "data['province_id'] provinceId, " +
            "`old` " +
            "from ods_base_db " +
            "where `table` = 'order_info' " +
            "and `type` = 'update' " +
            "and data['order_status']='1005'";


    /**
     * 三张表连接.
     */
    public static final String JOIN_TABLE = "select  " +
            "ri.id, " +
            "ri.user_id, " +
            "ri.order_id, " +
            "ri.sku_id, " +
            "oi.provinceId, " +
            "date_format(ri.create_time,'yyyy-MM-dd') date_id, " +
            "ri.create_time, " +
            "ri.refund_type, " +
            "type_dic.dic_name, " +
            "ri.refund_reason_type, " +
            "reason_dic.dic_name, " +
            "ri.refund_reason_txt, " +
            "ri.refund_num, " +
            "ri.refund_amount, " +
            "ri.ts, " +
            "current_row_timestamp() row_op_ts " +
            "from order_refund_info ri " +
            "left join  " +
            "order_info_refund oi " +
            "on ri.order_id = oi.id " +
            "left join  " +
            "base_dic for system_time as of ri.proc_time as type_dic " +
            "on ri.refund_type = type_dic.dic_code " +
            "left join " +
            "base_dic for system_time as of ri.proc_time as reason_dic " +
            "on ri.refund_reason_type=reason_dic.dic_code";

    /**
     * 退单明细宽表.
     */
    public static final String SINK_TOPIC_DDL = "create table dwd_trade_order_refund_detail( " +
            "id string, " +
            "user_id string, " +
            "order_id string, " +
            "sku_id string, " +
            "provinceId string, " +
            "date_id string, " +
            "create_time string, " +
            "refund_type_code string, " +
            "refund_type_name string, " +
            "refund_reason_type_code string, " +
            "refund_reason_type_name string, " +
            "refund_reason_txt string, " +
            "refund_num string, " +
            "refund_amount string, " +
            "ts string, " +
            "row_op_ts timestamp_ltz(3), " +
            "primary key(id) not enforced " +
            ")";
}
