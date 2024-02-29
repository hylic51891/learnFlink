package metrics;

import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @ClassNAME ScheduledTaskExample
 * @Description TODO
 * @Author hylic
 * @Date 2024/1/30 15:00
 * @Version 1.0
 */
public class ScheduledTaskExample {
    ArrayList<Integer> list = new ArrayList<>();
    ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
    public static void main(String[] args) throws InterruptedException {

        ScheduledTaskExample scheduledTaskExample = new ScheduledTaskExample();
        // 创建 ScheduledExecutorService

        // 初始延迟为 0，执行频率为每隔 1 秒执行一次
        long initialDelay = 0;
        long period = 1;
        scheduledTaskExample.initMetrics();
        // 使用 scheduleAtFixedRate 方法调度任务
        scheduledTaskExample.run(initialDelay,period);
        // 暂停执行 3 秒
        try {
            Thread.sleep(3000); // 单位是毫秒
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        scheduledTaskExample.simulateAddMetric();
    }
    public void initMetrics(){
        list.add(3);
        list.add(4);
        list.add(5);
    }
    public void run(long initialDelay, long period){
        executorService.scheduleAtFixedRate(this::task, initialDelay, period, TimeUnit.SECONDS);
    }
    public void simulateAddMetric() throws InterruptedException {
        list.add(6);
        list.add(7);
        list.add(8);
    }
    public void task(){
        for(int num:list){
            System.out.println(num);
        }
        System.out.println("Task is running...");
    }
}
