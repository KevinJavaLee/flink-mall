package cn.vinlee.app.sql.dwd;

/**
 * 用户注册表.
 *
 * @author Vinlee Xiao
 * @className DwdUserRegisterSql
 * @date 2022/7/26 21:38:43
 **/
public class DwdUserRegisterSql {
    private DwdUserRegisterSql() {

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
     * 用户信息表.
     */
    public static final String USER_INFO = "select " +
            "data['id'] user_id, " +
            "data['create_time'] create_time, " +
            "ts " +
            "from ods_base_db " +
            "where `table` = 'user_info' " +
            "and `type` = 'insert' ";

    public static final String SINK_TOPIC = "create table `dwd_user_register_detail`" +
            "( " +
            "`user_id` string, " +
            "`date_id` string, " +
            "`create_time` string, " +
            "`ts` string, " +
            "primary key(`user_id`) not enforced " +
            ")";

}
