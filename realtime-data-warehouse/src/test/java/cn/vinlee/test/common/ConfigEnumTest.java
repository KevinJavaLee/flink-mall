package cn.vinlee.test.common;

import cn.vinlee.common.PhoenixConfigEnum;
import org.junit.Test;

/**
 * @author Vinlee Xiao
 * @className ConfigEnumTest
 * @date 2022/7/15 20:40:46
 **/
public class ConfigEnumTest {
    @Test
    public void configTest() {
        System.out.println(PhoenixConfigEnum.HBASE_SCHEMA);
        System.out.println(PhoenixConfigEnum.PHOENIX_DRIVER);
    }
}
