package cn.vinlee.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * 分词工具类.
 *
 * @author Vinlee Xiao
 * @className KeyWordUtils
 * @date 2022/7/29 20:04:02
 **/
public class KeyWordUtil {

    private KeyWordUtil() {

    }

    public static List<String> splitKeyWord(String keyword) throws IOException {

        //存放最终的结果.
        ArrayList<String> result = new ArrayList<>();

        //创建分词器对象
        StringReader reader = new StringReader(keyword);
        IKSegmenter ikSegmenter = new IKSegmenter(reader, false);

        //提取分词
        Lexeme next = ikSegmenter.next();

        while (next != null) {
            String word = next.getLexemeText();
            result.add(word);

            next = ikSegmenter.next();
        }

        //返回结果
        return result;
    }
}
