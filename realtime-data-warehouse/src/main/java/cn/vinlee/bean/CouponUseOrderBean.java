package cn.vinlee.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Vinlee Xiao
 * @className CouponUseOrderBean
 * @date 2022/7/26 19:59:11
 **/

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CouponUseOrderBean {

    /**
     * 优惠券领用记录id.
     */
    String id;

    /**
     * 优惠券id.
     */
    String couponId;

    /**
     * 用户id.
     */
    String userId;

    /**
     * 订单id.
     */
    String orderId;

    /**
     * 优惠券领取日期.
     */
    String dateId;

    /**
     * 优惠券使用日期.
     */
    String usingTime;

    /**
     * 历史数据.
     */
    String old;

    /**
     * 时间戳.
     */
    String ts;


}
