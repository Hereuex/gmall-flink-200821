package com.atguigu.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.security.SecureRandom;
import java.util.List;

/**
 * @author Hereuex
 * @date 2021/2/27 14:11
 */
public class DimUtil {

    public static JSONObject getDimInfo(String tableName, Tuple2<String, String>... columnValues) {

        if (columnValues.length <= 0) {
            throw new RuntimeException("查询维度数据时，请至少设置一个查询条件！");
        }

        //创建Phoenix Where子句
        StringBuilder whereSql = new StringBuilder(" where ");

        //创建RedisKey
        StringBuilder redisKey = new StringBuilder(tableName).append(":");


        //遍历查询条件并赋值whereSql
        for (int i = 0; i < columnValues.length; i++) {
            //获取单个查询条件
            Tuple2<String, String> columnValue = columnValues[i];

            String column = columnValue.f0;
            String value = columnValue.f1;
            whereSql.append(column).append("='").append(value).append("'");

            //判断如果不是最后一个条件，则添加"and"
            if (i < columnValues.length - 1) {
                whereSql.append(" and ");


            }
        }

        Jedis jedis = RedisUtil.getJedis();
        String dimJsonStr = jedis.get(redisKey.toString());

        if (dimJsonStr != null && dimJsonStr.length() > 0) {
            jedis.close();
            return JSON.parseObject(dimJsonStr);
        }

        //拼接SQL
        String querySql = "select * from " + tableName + whereSql.toString();
        System.out.println(querySql);

        //查询phoenix中的维度数据
        List<JSONObject> queryList = PhoenixUtil.queryList(querySql, JSONObject.class);
        JSONObject dimJsonObj = queryList.get(0);


        return dimJsonObj;
    }


    public static JSONObject getDimInfo(String tableName, String value) {
        return getDimInfo(tableName, new Tuple2<>("id", value));
    }

    public static void main(String[] args) {
        System.out.println(getDimInfo("DIM_BASE_TRADEMARK", "15"));
    }


}
