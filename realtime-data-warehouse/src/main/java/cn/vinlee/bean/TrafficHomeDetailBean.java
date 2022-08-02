package cn.vinlee.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Vinlee Xiao
 * @className TrafficHomeDetailBean
 * @date 2022/7/30 22:31:29
 **/
@AllArgsConstructor
@NoArgsConstructor
@Data
public class TrafficHomeDetailBean {

    /**
     * 窗口起始时间.
     */
    String startTime;
    /**
     * 窗口结束时间.
     */
    String endTime;
    /**
     * 首页独立访客数.
     */
    Long homeUserVisitorCount;
    /**
     * 商品详情页独立访客数.
     */
    Long goodDetailUserVisitorCount;
    /**
     * 时间戳.
     */
    Long ts;
}
