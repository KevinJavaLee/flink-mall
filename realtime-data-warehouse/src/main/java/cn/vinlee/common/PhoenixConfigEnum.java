package cn.vinlee.common;

/**
 * Phoenix配置枚举类
 *
 * @author Vinlee Xiao
 * @className PhoenixConfigEnum
 * @date 2022/7/15 20:31:21
 **/
public enum PhoenixConfigEnum {

    /**
     * Phoenix库名.
     */
    HBASE_SCHEMA("GMALL_REALTIME"),
    /**
     * Phoenix驱动.
     */
    PHOENIX_DRIVER("org.apache.phoenix.jdbc.PhoenixDriver"),

    /**
     * Phoenix服务器.
     */
    PHOENIX_SERVER("jdbc:phoenix:hadoop1,hadoop2,hadoop3:2181");

    /**
     * 配置的名称.
     */
    private final String name;

    PhoenixConfigEnum(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
