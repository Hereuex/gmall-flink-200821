package com.atguigu.common;

/**
 * @author Hereuex
 * @date 2021/2/23 13:59
 */
public class GmallConfig {

    //Phoenix库名称
    public static final String HBASE_SCHEMA = "GMALL200821_REALTIME";

    //Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    //phoenix连接地址
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";

    //ClickHouse驱动
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

    //ClickHouse连接地址
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop102:8123/default";


}
