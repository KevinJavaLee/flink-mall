package cn.vinlee.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * TableProcessConfig对象.
 *
 * @author Vinlee Xiao
 * @className TableProcess
 * @date 2022/7/15 20:15:47
 **/
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TableProcessConfig {


    private String sourceTable;

    private String sinkTable;
    private String sinkColumns;
    private String sinkPk;
    private String sinkExtend;
}
