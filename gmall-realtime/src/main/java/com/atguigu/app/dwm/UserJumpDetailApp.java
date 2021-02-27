package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @author Hereuex
 * @date 2021/2/25 16:27
 */
public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {

        //1、获取执行环境，设置并行度，开启CK，设置状态后端（HDFS）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //为Kafka主题的分区数
        env.setParallelism(1);
        //        //1.1设置状态后端
        //        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/dwd_log/ck"));
        //        //1.2开启CK
        //        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        //        env.getCheckpointConfig().setAlignmentTimeout(60000L);

        //2、读取kafka dwd_page_log 主题数据创建流
        String sourceTopic = "dwd_page_log";
        String groupId = "userJumpDetailApp";
        String sinkTopic = "dwm_user_jump_detail";

        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

       // DataStream<String> kafkaDS = env.socketTextStream("hadoop102",9999);

        WatermarkStrategy<JSONObject> watermarkStrategy = WatermarkStrategy
                .<JSONObject>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                });

        //将数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {

            @Override
            public void processElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(new OutputTag<String>("dirty") {
                    }, s);
                }
            }
        }).assignTimestampsAndWatermarks(watermarkStrategy);


        //4.按照Mid进行分区
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //5.定义模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                //取出上一条页面信息
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                return lastPageId == null || lastPageId.length() <= 0;
            }
        }).followedBy("follow").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                //取出上一条页面信息
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                return lastPageId != null && lastPageId.length() > 0;
            }
        }).within(Time.seconds(10));

        //6.将模式序列作用在流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        //7.提取事件和超时事件
        OutputTag<String> timeOutTag = new OutputTag<String>("TimeOut") {
        };
        SingleOutputStreamOperator<Object> selectDS = patternStream.flatSelect(timeOutTag,
                new PatternFlatTimeoutFunction<JSONObject, String>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> map, long l, Collector<String> collector) throws Exception {
                        collector.collect(map.get("start").get(0).toString());
                    }
                }, new PatternFlatSelectFunction<JSONObject, Object>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> map, Collector<Object> collector) throws Exception {
                        //什么都不写,因为匹配上的数据是不需要的数据
                    }
                });

        //8.将数据写入Kafka
        FlinkKafkaProducer<String> kafkaSink = MyKafkaUtil.getKafkaSink(sinkTopic);
        selectDS.getSideOutput(timeOutTag).addSink(kafkaSink);


        env.execute();
    }
}
