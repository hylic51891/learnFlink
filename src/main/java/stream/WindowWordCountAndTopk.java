package stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;

public class WindowWordCountAndTopk {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String,Integer>> count = env
                .socketTextStream("localhost", 9999)
                .flatMap(new Splitter())
                .keyBy(value -> value.f0) // 以tuple中的第一个元素作为key进行分类
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1);

        DataStream<String>  topk =  count
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new TopkWindowFunction(3));
        count.print();
        topk.print();
        env.execute("Window WordCount");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

    public static class TopkWindowFunction extends ProcessAllWindowFunction<Tuple2<String, Integer>, String, TimeWindow> {
        private int size = 5;
        public TopkWindowFunction(){

        }

        public TopkWindowFunction(int size){
            this.size = size;
        }
        @Override
        public void process( Context context, Iterable<Tuple2<String, Integer>> input, Collector<String> out) throws Exception {
            PriorityQueue<Tuple2<String, Integer>> pq = new PriorityQueue<>(new Comparator<Tuple2<String, Integer>>() {
                @Override
                public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
                    return o1.f1-o2.f1;
                }
            });

            for(Tuple2<String, Integer> pair:input){
                pq.add(pair);
                if(pq.size()>size) pq.poll();
            }
            int idx = 0;
            StringBuilder sb = new StringBuilder();
            while(!pq.isEmpty()){
                Tuple2<String, Integer> t = pq.poll();
                sb.insert(0," rank: " + (size-idx) + " name: " + t.f0 + " freq: " + t.f1 + "\n");
                idx++;
            }
            out.collect(sb.toString());

        }
    }
}