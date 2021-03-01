package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.OrderDetail;
import com.atguigu.bean.OrderInfo;
import com.atguigu.bean.OrderWide;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

/**
 * @author Hereuex
 * @date 2021/2/26 10:46
 */
public class OrderWideApp {


    public static void main(String[] args) throws Exception {


        //1、创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //为Kafka主题的分区数
        env.setParallelism(1);
        //1.1设置状态后端
        //        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/dwd_log/ck"));
        //1.2开启CK
        //        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        //        env.getCheckpointConfig().setAlignmentTimeout(60000L);

        //2、读取Kafka订单和订单明细主题数据 dwd_order_info dwd_order_detail
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";


        FlinkKafkaConsumer<String> orderInfoKafkaSource = MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId);
        DataStreamSource<String> orderInfoKafkaDS = env.addSource(orderInfoKafkaSource);
        FlinkKafkaConsumer<String> orderDetailKafkaSource = MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId);
        DataStreamSource<String> orderDetailKafkaDS = env.addSource(orderDetailKafkaSource);

        //3、将每行数据转化为JavaBean,提取时间戳生成WaterMark
        WatermarkStrategy<OrderInfo> orderInfoWatermarkStrategy = WatermarkStrategy.<OrderInfo>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
            @Override
            public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                return element.getCreate_ts();
            }
        });

        WatermarkStrategy<OrderDetail> orderDetailWatermarkStrategy = WatermarkStrategy.<OrderDetail>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
            @Override
            public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                return element.getCreate_ts();
            }
        });

        KeyedStream<OrderInfo, Long> orderInfoWithIdKeyedStream = orderInfoKafkaDS.map(jsonStr -> {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            OrderInfo orderInfo = JSON.parseObject(jsonStr, OrderInfo.class);
            String create_time = orderInfo.getCreate_time();
            String[] createTimeArr = create_time.split(" ");

            orderInfo.setCreate_date(createTimeArr[0]);
            orderInfo.setCreate_hour(createTimeArr[1]);
            orderInfo.setCreate_ts(sdf.parse(create_time).getTime());

            return orderInfo;
        }).assignTimestampsAndWatermarks(orderInfoWatermarkStrategy).keyBy(OrderInfo::getId);

        KeyedStream<OrderDetail, Long> orderDetailWithIdKeyedStream = orderDetailKafkaDS.map(jsonStr -> {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            OrderDetail orderDetail = JSON.parseObject(jsonStr, OrderDetail.class);
            orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time()).getTime());
            System.out.println(">>>>>>>>>>>>>>>>>");
            return orderDetail;
        }).assignTimestampsAndWatermarks(orderDetailWatermarkStrategy).keyBy(OrderDetail::getOrder_id);

        //4、双流join
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoWithIdKeyedStream.intervalJoin(orderDetailWithIdKeyedStream)
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
            @Override
            public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context context, Collector<OrderWide> collector) throws Exception {
                collector.collect(new OrderWide(orderInfo, orderDetail));
            }
        });

        orderWideDS.print(">>>>>>>>>");

        //5.1 关联用户维度




        //7、开启任务
        env.execute();

    }

}
