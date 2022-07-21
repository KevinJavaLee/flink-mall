package cn.vinlee.utils;

/**
 * 时间转换工具类.
 *
 * @author Vinlee Xiao
 * @className DateFormatUtil
 * @date 2022/7/19 20:12:12
 **/

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DateFormatUtil {

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter DATE_TIME_FORMATTER_FULL = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static Long toTimestamp(String dateStr, boolean isFull) {

        LocalDateTime localDateTime = null;
        if (!isFull) {
            dateStr = dateStr + " 00:00:00";
        }
        localDateTime = LocalDateTime.parse(dateStr, DATE_TIME_FORMATTER_FULL);

        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }

    public static Long toTimestamp(String dtStr) {
        return toTimestamp(dtStr, false);
    }

    public static String toDate(Long ts) {
        Date dt = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        return DATE_TIME_FORMATTER.format(localDateTime);
    }

    public static String toYmdHms(Long ts) {
        Date dt = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        return DATE_TIME_FORMATTER_FULL.format(localDateTime);
    }
    
}

