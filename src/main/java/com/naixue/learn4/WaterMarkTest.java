package com.naixue.learn4;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class WaterMarkTest {

}

class EventTimeExtractor implements AssignerWithPeriodicWatermarks{

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return null;
    }

    @Override
    public long extractTimestamp(Object element, long recordTimestamp) {
        return 0;
    }
}