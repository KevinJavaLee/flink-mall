package cn.vinlee.app.func.dws;

import cn.vinlee.utils.KeyWordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

/**
 * 分词函数.
 *
 * @author Vinlee Xiao
 * @className SplitFunction
 * @date 2022/7/29 20:08:25
 **/
@FunctionHint(output = @DataTypeHint("Row<word STRING>"))
public class SplitFunction extends TableFunction<Row> {

    public void eval(String keyword) {

        try {
            List<String> wordList = KeyWordUtil.splitKeyWord(keyword);

            for (String word : wordList) {
                collect(Row.of(word));
            }
        } catch (IOException e) {
            collect(Row.of(keyword));
            
        }
    }
}
