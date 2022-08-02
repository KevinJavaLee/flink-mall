package cn.vinlee.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Vinlee Xiao
 * @className UserRegisterBean
 * @date 2022/7/31 18:07:02
 **/
@AllArgsConstructor
@NoArgsConstructor
@Data
public class UserRegisterBean {

    /**
     * 窗口起始时间.
     */
    String startTime;
    /**
     * 窗口终止时间.
     */
    String endTime;
    /**
     * 注册用户数.
     */
    Long registerUserCount;
    /**
     * 时间戳.
     */
    Long ts;
}
