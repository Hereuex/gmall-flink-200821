package com.atguigu.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Hereuex
 * @date 2021/2/26 20:56
 */
public class PhoenixUtil {

    //声明
    private static Connection connection;

    private static Connection init() {
        try {
            Class.forName(GmallConfig.PHOENIX_DRIVER);
            Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

            //设置连接到Phoenix的库
            connection.setSchema(GmallConfig.HBASE_SCHEMA);


            return connection;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("获取连接失败！");
        }
    }

    public static <T> List<T> queryList(String sql, Class<T> cls) {
        //初始化连接
        if (connection == null) {
            connection = init();
        }

        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            preparedStatement = connection.prepareStatement(sql);

            resultSet = preparedStatement.executeQuery();

            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            ArrayList<T> list = new ArrayList<>();

            while (resultSet.next()) {
                T t = cls.newInstance();
                for (int i = 1; i < columnCount + 1; i++) {
                    BeanUtils.setProperty(t, metaData.getColumnName(i), resultSet.getObject(i));

                }
                list.add(t);
            }
            return list;

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("查询维度信息失败！");
        } finally {
            if (preparedStatement != null) {

                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    public static void main(String[] args) {
        System.out.println(queryList("select * from DIM_BASE_TRADEMARK", JSONObject.class));
    }


}
