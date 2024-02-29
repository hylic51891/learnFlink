package com.naixue.learn3;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class CustomSink implements SinkFunction<Tuple2<String, Integer>>, CheckpointedFunction {
    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

    }

    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        SinkFunction.super.invoke(value, context);
    }
}
