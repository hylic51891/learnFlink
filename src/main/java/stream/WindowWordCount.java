package stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.PriorityQueue;
import java.util.Random;

// 统计一定时间内输入的数据的出现频率，并且输出topK的值
// 数据源1 nc -lk 9999 在控制台输入数据
// 数据源2 随机生成数字 0-10
public class WindowWordCount {
    static final int TIMER_DELAY = 1000;
    static final int TOPK_SIZE = 5;


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<Long, String, Integer>> window_Item_count = env
                .addSource(new MyDataSource())//                .socketTextStream("localhost", 9999)
                .flatMap(new Splitter())
                .keyBy(value -> value.f0) // 以tuple中的第一个元素作为key进行分类
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .reduce(new MyReduceFunction(), new MyProcessWindowFunction());

        window_Item_count.print();

        DataStream<Tuple2<Long, String>> topK = window_Item_count
                .keyBy(tuple -> tuple.f0)
                .process(new topkProcessFunction(TOPK_SIZE));


        topK.print();

        env.execute("Window WordCount");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word : sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

    private static class MyReduceFunction implements ReduceFunction<Tuple2<String, Integer>> {

        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> r1, Tuple2<String, Integer> r2) {
            return new Tuple2<>(r1.f0, r1.f1 + r2.f1);
        }
    }

    private static class topkProcessFunction
            extends KeyedProcessFunction<Long, Tuple3<Long, String, Integer>, Tuple2<Long, String>> {
        private int size;
        private PriorityQueue<Tuple2<String, Integer>> pq;

        public topkProcessFunction() {
        }

        public topkProcessFunction(int size) {
            this.size = size;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            pq = new PriorityQueue<>((o1, o2) -> o1.f1 - o2.f1); // 在开启的时候进行初始化
        }

        @Override
        public void processElement(Tuple3<Long, String, Integer> in,
                                   Context context, Collector<Tuple2<Long, String>> out) throws Exception {
            pq.add(new Tuple2<>(in.f1, in.f2));
            if (pq.size() > size) pq.poll();
            context.timerService().registerProcessingTimeTimer(in.f0 + TIMER_DELAY);
        }

        @Override
        public void onTimer(long timestamp,
                            OnTimerContext ctx,
                            Collector<Tuple2<Long, String>> out) throws Exception {
            int idx = pq.size();
            StringBuilder sb = new StringBuilder();
            while (!pq.isEmpty()) {
                Tuple2<String, Integer> t = pq.poll();
                sb.insert(0, " rank: " + idx + " name: " + t.f0 + " freq: " + t.f1 + "\n");
                idx--;
            }
            out.collect(new Tuple2<>(timestamp - TIMER_DELAY, sb.toString()));
        }
    }

    private static class MyProcessWindowFunction
            extends ProcessWindowFunction<Tuple2<String, Integer>, Tuple3<Long, String, Integer>, String, TimeWindow> {
        @Override
        public void process(String key,
                            Context context,
                            Iterable<Tuple2<String, Integer>> words,
                            Collector<Tuple3<Long, String, Integer>> out) throws Exception {
            Tuple2<String, Integer> t = words.iterator().next();
            out.collect(new Tuple3<>(context.window().getEnd(), t.f0, t.f1));
        }
    }


    private static class MyDataSource implements SourceFunction<String> {
        // 定义标志位，用来控制数据的产生
        private boolean isRunning = true;
        private final Random random = new Random(0);

        @Override
        public void run(SourceContext ctx) throws Exception {
            while (isRunning) {
                ctx.collect(String.valueOf(random.nextInt(10)));
                Thread.sleep(50L); // 1s生成1个数据
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}