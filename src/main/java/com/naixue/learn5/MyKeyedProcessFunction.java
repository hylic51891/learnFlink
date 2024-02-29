package com.naixue.learn5;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class MyKeyedProcessFunction extends KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Object> {
    private ValueState<CountWithTimeStamp> valueState;
    private static final int GAP  = 5000;
    private static class CountWithTimeStamp{
        String key;
        Integer count;
        long lastModified;

        public CountWithTimeStamp(String key, Integer count, long lastModified) {
            this.key = key;
            this.count = count;
            this.lastModified = lastModified;
        }
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        valueState = getRuntimeContext().getState(new ValueStateDescriptor<CountWithTimeStamp>("myState",CountWithTimeStamp.class));

    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Object>.OnTimerContext ctx, Collector<Object> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        CountWithTimeStamp cur = valueState.value();
        if(timestamp == cur.lastModified+GAP){
            out.collect(new Tuple2<>(cur.key,cur.count));
            valueState.clear();
        }
    }

    @Override
    public void processElement(Tuple2<String, Integer> value, KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Object>.Context ctx, Collector<Object> out) throws Exception {
        CountWithTimeStamp curElement = valueState.value();

        if(curElement==null){
            curElement = new CountWithTimeStamp(value.f0,0,ctx.timerService().currentProcessingTime());
        }
        curElement.count++;
        curElement.lastModified = ctx.timerService().currentProcessingTime();

        valueState.update(curElement);

        ctx.timerService().registerProcessingTimeTimer(curElement.lastModified+GAP);
    }
}