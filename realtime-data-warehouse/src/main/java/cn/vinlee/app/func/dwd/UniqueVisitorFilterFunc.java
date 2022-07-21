package cn.vinlee.app.func.dwd;

import cn.vinlee.utils.DateFormatUtil;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;

/**
 * 对独立访问数据进行过滤.
 *
 * @author Vinlee Xiao
 * @className UniqueVisitorFilterFunc
 * @date 2022/7/20 21:19:06
 **/
public class UniqueVisitorFilterFunc extends RichFilterFunction<JSONObject> {

    private transient ValueState<String> visitDtState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("visit-dt", String.class);

        //设置状态TTL
        StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.days(1))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .build();
        stateDescriptor.enableTimeToLive(stateTtlConfig);

        //初始化状态
        visitDtState = getRuntimeContext().getState(stateDescriptor);
    }

    @Override
    public boolean filter(JSONObject jsonObject) throws Exception {

        //取出当前状态以及当前数据的日期
        String dateTime = visitDtState.value();
        String currentDateTime = DateFormatUtil.toDate(jsonObject.getLong("ts"));

        //如果时间状态为空，或者当前时间不是当日则修改状态
        if (dateTime == null || !dateTime.equals(currentDateTime)) {
            visitDtState.update(currentDateTime);
            return true;
        } else {
            return false;
        }

    }
}
