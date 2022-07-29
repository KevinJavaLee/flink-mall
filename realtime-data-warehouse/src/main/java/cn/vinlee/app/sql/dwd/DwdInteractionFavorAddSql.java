package cn.vinlee.app.sql.dwd;

/**
 * 互动域收藏商品事实表.
 *
 * @author Vinlee Xiao
 * @className DwdInteractionFavorAddSql
 * @date 2022/7/26 21:07:53
 **/
public class DwdInteractionFavorAddSql {

    private DwdInteractionFavorAddSql() {

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
     * 收藏域表数据.
     */
    public static final String FAVOR_INFO_ADD = "select " +
            "data['id'] id, " +
            "data['user_id'] user_id, " +
            "data['sku_id'] sku_id, " +
            "date_format(data['create_time'],'yyyy-MM-dd') date_id, " +
            "data['create_time'] create_time, " +
            "ts " +
            "from ods_base_db " +
            "where `table` = 'favor_info' and `type` = 'insert' ";
//            +
//            "and `type` = 'insert' " +
//            "or (`type` = 'update' and `old`['is_cancel'] = '1' and data['is_cancel'] = '0')";


    public static final String SINK_TOPIC = "create table dwd_interaction_favor_add_detail ( " +
            "id string, " +
            "user_id string, " +
            "sku_id string, " +
            "date_id string, " +
            "create_time string, " +
            "ts string, " +
            "primary key(id) not enforced " +
            ")";
}
