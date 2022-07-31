package cn.vinlee.bean;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 注解用于表示某个Bean中的属性不需要插入到数据中.
 *
 * @author Vinlee Xiao
 * @className TransientSink
 * @date 2022/7/29 21:47:40
 **/
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface TransientSink {
}
