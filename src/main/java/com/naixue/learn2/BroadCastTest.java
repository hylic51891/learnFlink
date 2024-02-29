package com.naixue.learn2;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class BroadCastTest {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //1:准备需要广播的数据
        ArrayList<Tuple2<String, Integer>> broadData = new ArrayList<>(); broadData.add(new Tuple2<>("zs",18));
        broadData.add(new Tuple2<>("zs",18));
        broadData.add(new Tuple2<>("ls",20));
        broadData.add(new Tuple2<>("ww",17));
        DataSet<Tuple2<String, Integer>> tupleData = env.fromCollection(broadData);
        DataSet<HashMap<String, Integer>> toBroadcast = tupleData.map(new
            MapFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
                @Override
                public HashMap<String, Integer> map(Tuple2<String, Integer> value)
                      throws Exception {
                  HashMap<String, Integer> res = new HashMap<>();
                  res.put(value.f0, value.f1);
                  return res;
                }
        });
//源数据
        DataSource<String> data = env.fromElements("zs", "ls", "ww"); //注意:在这里需要使用到RichMapFunction获取广播变量
        DataSet<String> result = data.map(new RichMapFunction<String, String>() {
            List<HashMap<String,Integer>> broadMap = new ArrayList<>();
            HashMap<String,Integer> allMap = new HashMap<>();
            @Override
            public String map(String s) throws Exception {
                Integer age = allMap.get(s);
                return s + "," + age;
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                broadMap = getRuntimeContext().getBroadcastVariable("map");
                for(HashMap<String,Integer> hashMap: broadMap){
                    allMap.putAll(hashMap);
                }
            }
        }).withBroadcastSet(toBroadcast,"map");
        result.print();
    }
}
