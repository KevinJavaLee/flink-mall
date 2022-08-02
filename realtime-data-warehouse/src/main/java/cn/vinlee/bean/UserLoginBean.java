package cn.vinlee.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 用户登陆实体类.
 *
 * @author Vinlee Xiao
 * @className UserLoginBean
 * @date 2022/7/31 13:45:43
 **/
@AllArgsConstructor
@NoArgsConstructor
@Data
public class UserLoginBean {

    /**
     * 窗口起始时间.
     */
    String startTime;
    /**
     * 窗口终止时间.
     */
    String endTime;
    /**
     * 回流用户数.
     */
    Long backUserCount;
    /**
     * 独立用户数.
     */
    Long uniqueUserCount;
    /**
     * 时间戳.
     */
    Long ts;
}
