package cn.vinlee.app.func.dwd;

import cn.vinlee.utils.DateFormatUtil;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

/**
 * 通过Flink 状态来做新老用户的校验.
 *
 * @author Vinlee Xiao
 * @className ValidateOldNewCustomersFunc
 * @date 2022/7/19 20:23:58
 **/
public class ValidateOldNewCustomersFunc extends RichMapFunction<JSONObject, JSONObject> {

    private static final String NEW_VISITED_FLAG = "1";
    private transient ValueState<String> lastVisitState;


    @Override
    public void open(Configuration parameters) throws Exception {
        lastVisitState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-visit", String.class));

    }

    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {

        //1.获取"is_new"标记
        String newVisitedFlag = jsonObject.getJSONObject("common").getString("is_new");
        String lastVisitDataTime = lastVisitState.value();
        Long ts = jsonObject.getLong("ts");

        //2.判断是否为1
        if (NEW_VISITED_FLAG.equals(newVisitedFlag)) {

            //3.获取当前的数据时间
            String currentDateTime = DateFormatUtil.toDate(ts);

            //3.1上次访问时间状态为null，说明是首次访问,将状态置为当前ts
            if (lastVisitDataTime == null) {
                lastVisitState.update(currentDateTime);
            } else if (!lastVisitDataTime.equals(currentDateTime)) {
                //3.2如果上次访问时间不为null,且不等于当日，则说明是老访问，将is_new字段置为0
                jsonObject.getJSONObject("common").put("is_new", 0);
            }

            //3.3如果上次访问时间不为null,而且等于当日说明是新访问，不做处理。


        } else if (lastVisitDataTime == null) {
            //3.4 如果不为1，且键控状态为null,说明访问app的是老访客
            String yesterday = DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000L);
            //将日期置为昨天
            lastVisitState.update(yesterday);

            //3.5如果上次访问时间不为空，说明程序已经维护了其首次访问时间
        }


        return jsonObject;
    }
}
