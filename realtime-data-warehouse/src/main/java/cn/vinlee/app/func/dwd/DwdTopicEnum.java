package cn.vinlee.app.func.dwd;

/**
 * dwd层输出到的kafka主题。
 *
 * @author Vinlee Xiao
 * @className DwdTopicEnum
 * @date 2022/7/19 21:39:50
 **/
public enum DwdTopicEnum {

    /**
     * 页面日志主题.
     */
    DWD_TRAFFIC_PAGE_LOG("dwd_traffic_page_log"),

    /**
     * 启动页面日志主题.
     */
    DWD_TRAFFIC_START_LOG("dwd_traffic_start_log"),
    /**
     * 展示页面日志主题.
     */
    DWD_TRAFFIC_DISPLAY_LOG("dwd_traffic_display_log"),

    /**
     * 行动日志主题.
     */
    DWD_TRAFFIC_ACTION_LOG("dwd_traffic_action_log"),

    /**
     * 错误日志主题.
     */
    DWD_TRAFFIC_ERROR_LOG("dwd_traffic_error_log");

    private final String name;

    DwdTopicEnum(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
