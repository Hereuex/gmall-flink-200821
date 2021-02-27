package com.atguigu.app.dwm;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Hereuex
 * @date 2021/2/27 15:17
 */
public class PaymentWideApp {

    public static void main(String[] args) {

        //1、创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //为Kafka主题的分区数
        env.setParallelism(1);
        //1.1设置状态后端
        //        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/dwd_log/ck"));
        //1.2开启CK
        //        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        //        env.getCheckpointConfig().setAlignmentTimeout(60000L);

        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";



    }

}
