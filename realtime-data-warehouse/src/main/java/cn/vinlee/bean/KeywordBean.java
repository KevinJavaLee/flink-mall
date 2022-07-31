package cn.vinlee.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Vinlee Xiao
 * @className KeywordBean
 * @date 2022/7/29 20:24:04
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class KeywordBean {


    /**
     * 窗口起始事件
     */
    private String startTime;

    /**
     * 窗口结束事件.
     */
    private String endTime;

    /**
     * 关键字来源.
     */
    private String source;

    /**
     * 关键字.
     */
    private String keyword;

    /**
     * 关键词频度.
     */
    private Long keywordCount;

    /**
     * 时间戳.
     */
    private Long ts;
}
