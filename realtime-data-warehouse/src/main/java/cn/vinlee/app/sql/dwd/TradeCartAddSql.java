package cn.vinlee.app.sql.dwd;

/**
 * @author Vinlee Xiao
 * @className TradeCardAddSql
 * @date 2022/7/23 15:27:41
 **/
public class TradeCartAddSql {
    private TradeCartAddSql() {

    }

    /**
     * 加购信息表.
     */
    public static final String CART_ADD = "" +
            "select " +
            "    data['id'] id, " +
            "    data['user_id'] user_id, " +
            "    data['sku_id'] sku_id, " +
            "    data['cart_price'] cart_price, " +
            "    if(`type`='insert',cast(`data`['sku_num'] as int),cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int)) sku_num, " +
            "    data['sku_name'] sku_name, " +
            "    data['is_checked'] is_checked, " +
            "    data['create_time'] create_time, " +
            "    data['operate_time'] operate_time, " +
            "    data['is_ordered'] is_ordered, " +
            "    data['order_time'] order_time, " +
            "    data['source_type'] source_type, " +
            "    data['source_id'] source_id, " +
            "    pt " +
            "from ods_base_db " +
            "where `database`='gmall' " +
            "and `table`='cart_info' " +
            "and (`type`='insert'  " +
            "    or (`type`='update'  " +
            "        and `old`['sku_num'] is not null " +
            "        and cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int)))";


    public static final String CART_ADD_INFO = "" +
            "select \n" +
            "    c.id, \n" +
            "    c.user_id, \n" +
            "    c.sku_id, \n" +
            "    c.cart_price, \n" +
            "    c.sku_num, \n" +
            "    c.sku_name, \n" +
            "    c.is_checked, \n" +
            "    c.create_time, \n" +
            "    c.operate_time, \n" +
            "    c.is_ordered, \n" +
            "    c.order_time, \n" +
            "    c.source_type, \n" +
            "    c.source_id, \n" +
            "    b.dic_name \n" +
            "from cart_add c \n" +
            "join base_dic FOR SYSTEM_TIME AS OF c.pt as b \n" +
            "on c.source_type = b.dic_code\n";

    public static final String SINK_TRADE_CARD_INFO = "" +
            "create table trade_cart_add( \n" +
            "    id string,\n " +
            "    user_id string, \n" +
            "    sku_id string, \n" +
            "    cart_price string, \n" +
            "    sku_num int, \n" +
            "    sku_name string, \n" +
            "    is_checked string, \n" +
            "    create_time string, \n" +
            "    operate_time string, \n" +
            "    is_ordered string, \n" +
            "    order_time string, \n" +
            "    source_type string, \n" +
            "    source_id string, \n" +
            "    dic_name string \n" +
            ")\n";
}
