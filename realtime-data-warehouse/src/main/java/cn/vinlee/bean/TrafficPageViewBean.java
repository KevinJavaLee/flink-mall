package cn.vinlee.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 页面浏览类.
 *
 * @author Vinlee Xiao
 * @className TrafficPageViewBean
 * @date 2022/7/30 15:06:54
 **/

@AllArgsConstructor
@NoArgsConstructor
@Data
public class TrafficPageViewBean {

    /**
     * 窗口起始时间
     */
    private String startTime;
    /**
     * 窗口结束时间.
     */
    private String endTime;
    /**
     * app 版本号.
     */
    private String versionChannel;
    /**
     * 渠道.
     */
    private String channel;
    /**
     * 地区.
     */
    private String region;
    /**
     * 新老访客状态标记
     */
    private String isNew;
    /**
     * 独立访客数.
     */
    private Long uniqueVisitorCount;
    /**
     * 会话数.
     */
    private Long sessionVisitorCount;

    /**
     * 页面浏览数.
     */
    private Long pageVisitorCount;

    /**
     * 累计访问时长.
     */
    private Long duringSum;

    /**
     * 跳出会话数.
     */
    private Long userJumpCount;
    /**
     * 时间戳.
     */
    Long ts;
}
