package cn.vinlee.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 订单加购实体类.
 *
 * @author Vinlee Xiao
 * @className CartAddUniqueBean
 * @date 2022/7/31 20:24:58
 **/
@AllArgsConstructor
@NoArgsConstructor
@Data
public class CartAddUniqueBean {

    /**
     * 窗口起始时间.
     */
    String startTime;
    /**
     * 窗口结束时间.
     */
    String endTime;
    /**
     * 加购独立用户数.
     */
    Long cartAddUniqueCount;
    /**
     * 时间戳.
     */
    Long ts;
}
