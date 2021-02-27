package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.MySQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.*;
import java.util.*;

/**
 * @author Hereuex
 * @date 2021/2/23 11:34
 */
public class DbSplitProcessFunction extends ProcessFunction<JSONObject, JSONObject> {

    //定义属性
    private OutputTag<JSONObject> outputTag;

    //定义配置信息的Map
    private HashMap<String, TableProcess> tableProcessHashMap;

    //定义Set用于记录当前Phoenix中已经存在的表
    private HashSet<String> existTables;

    //定义phoenix的连接
    Connection connection = null;

    public DbSplitProcessFunction(OutputTag<JSONObject> outputTag) {
        this.outputTag = outputTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        //初始化配置信息的Map
        tableProcessHashMap = new HashMap<>();

        //初始化Phoenix已经存在的表的Set
        existTables = new HashSet<>();


        //初始化Phoenix的连接
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        //读取配置信息
        refreshMeta();

        //开启定时调度任务，周期性执行读取配置信息方法
        Timer time = new Timer();
        time.schedule(new TimerTask() {
            @Override
            public void run() {
                refreshMeta();
            }
        }, 10000L, 5000L);


    }

    /**
     * 用于周期性的读取配置信息
     */

    public void refreshMeta() {

        System.out.println("开始读取Mysql配置信息！");

        //1、读取Mysql中的配置信息
        List<TableProcess> tableProcesses = MySQLUtil.queryList("select * from table_process", TableProcess.class, true);

        //2、将查询结果封装为Map，以便于后续每条数据读取
        for (TableProcess tableProcess : tableProcesses) {

            //获取源表
            String sourceTable = tableProcess.getSourceTable();

            //读取操作类型
            String operateType = tableProcess.getOperateType();
            //设置key
            String key = sourceTable + ":" + operateType;
            tableProcessHashMap.put(key, tableProcess);

            //检查phoenix中是否存在该表,不存在创建该表
            if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {

                boolean noExist = existTables.add(tableProcess.getSinkTable());

                if (noExist) {
                    checkTable(tableProcess.getSinkTable(), tableProcess.getSinkColumns(), tableProcess.getSinkPk(), tableProcess.getSinkExtend());
                }
            }
        }
        //校验
        if (tableProcessHashMap == null || tableProcessHashMap.size() == 0) {
            throw new RuntimeException("读取MySql配置信息失败！！！");
        }
    }

    /***
     *
     * @param sinkTable
     * @param sinkColumns
     * @param sinkPk
     * @param sinkExtend
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        if (sinkPk == null) {
            sinkPk = "id";
        }

        if (sinkExtend == null) {
            sinkExtend = "";
        }


        //分装建表Sql
        StringBuilder createSql = new StringBuilder("create table if not exists ").append(GmallConfig.HBASE_SCHEMA).append(".").append(sinkTable).append("(");

        String[] fields = sinkColumns.split(",");
        for (int i = 0; i < fields.length; i++) {

            //取出字段
            String field = fields[i];

            //判断当前字段是否为主键
            if (sinkPk.equals(field)) {
                createSql.append(field).append(" varchar primary key ");
            } else {
                createSql.append(field).append(" varchar ");
            }

            //如果当前字段不是最后一个字段，则追加","
            if (i < fields.length - 1) {
                createSql.append(",");
            }
        }

        createSql.append(")");
        createSql.append(sinkExtend);

        System.out.println(createSql);
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(createSql.toString());
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("创建Phoenix表" + sinkTable + "失败");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    @Override
    public void processElement(JSONObject jsonObject, Context context, Collector<JSONObject> collector) throws Exception {
        //取出数据中的表名和操作类型
        String table = jsonObject.getString("table");
        String type = jsonObject.getString("type");

        //使用MaxWell初始化功能时，数据的操作类型为：bootstrap-insert

        if ("bootstrap-insert".equals(type)) {
            type = "insert";
            jsonObject.put("type", type);
        }

        //拼接key
        String key = table + ":" + type;

        //获取对应的tableProcess数据
        TableProcess tableProcess = tableProcessHashMap.get(key);

        //判断当前的配置信息是否存在
        if (tableProcess != null) {

            //向数据中追加sink_table信息
            jsonObject.put("sink_table", tableProcess.getSinkTable());

            //根据配置信息中提供的字段做数据过滤
            filterColumn(jsonObject.getJSONObject("data"), tableProcess.getSinkColumns());


            //判断写入数据流

            if (TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())) {
                collector.collect(jsonObject);
            } else if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                context.output(outputTag, jsonObject);
            }

        } else {
            System.out.println("No Key" + key + "In Mysql!");
        }

    }


    //根据配置信息中提供的字段做数据过滤
    private void filterColumn(JSONObject data, String sinkColumns) {

        //保留的数据
        String[] fields = sinkColumns.split(",");
        List<String> fieldList = Arrays.asList(fields);


        Set<Map.Entry<String, Object>> entries = data.entrySet();

//                while (entries.hasNext()) {
//                    Map.Entry<String, Object> next = entries.next();
//                    if (!fieldList.contains(next.getKey())) {
//                        entries.remove();
//                    }
//                }

        entries.removeIf(next -> !fieldList.contains(next.getKey()));


    }
}
