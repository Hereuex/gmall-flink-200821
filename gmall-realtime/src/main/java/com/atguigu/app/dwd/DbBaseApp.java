package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DbSplitProcessFunction;
import com.atguigu.app.func.DimSink;
import com.atguigu.bean.TableProcess;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @author Hereuex
 * @date 2021/2/22 10:21
 */
public class DbBaseApp {
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

        //2、读取Kafka数据
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource("ods_base_db_m", "ods_db_group");
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        //3、将每行数据转换成JsonObject
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSONObject::parseObject);

        //4.过滤
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                //获取data字段
                String data = value.getString("data");
                return data != null && data.length() > 0;
            }
        });

        //打印测试
        filterDS.print();

        //5.分流,ProcessFunction
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE) {
        };
        SingleOutputStreamOperator<JSONObject> kafkaJsonDS = filterDS.process(new DbSplitProcessFunction(hbaseTag));

        DataStream<JSONObject> hbaseJsonDS = kafkaJsonDS.getSideOutput(hbaseTag);

        //打印
        //        kafkaJsonDS.print("kafka>>>>>>>>>");
        //        hbaseJsonDS.print("hbase>>>>>>>>>");

        //6.取出分流输出将数据写入Kafka或者Phoenix

        hbaseJsonDS.addSink(new DimSink());
        FlinkKafkaProducer<JSONObject> kafkaSinkBySchema = MyKafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {

            @Override
            public void open(SerializationSchema.InitializationContext context) throws Exception {
                System.out.println("开始序列化Kafka数据！");
            }

            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                return new ProducerRecord<byte[], byte[]>(element.getString("sink_table"), element.getString("data").getBytes());
            }
        });
        kafkaJsonDS.addSink(kafkaSinkBySchema);
        //7.执行任务
        env.execute();


    }

}
