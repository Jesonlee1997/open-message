package io.openmessaging.demo.serialize;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by JesonLee
 * on 2017/5/3.
 */
public class Constants {
    //MessageHeader字段的编号
    public static final byte MESSAGE_ID = 1;
    public static final byte TOPIC = 2;
    public static final byte QUEUE = 3;
    public static final byte BORN_TIMESTAMP = 4;
    public static final byte BORN_HOST = 5;
    public static final byte STORE_TIMESTAMP = 6;
    public static final byte STORE_HOST = 7;
    public static final byte START_TIME = 8;
    public static final byte STOP_TIME = 9;
    public static final byte TIMEOUT = 10;
    public static final byte PRIORITY = 11;
    public static final byte RELIABILITY = 12;
    public static final byte SEARCH_KEY = 13;
    public static final byte SCHEDULE_EXPRESSION = 14;
    public static final byte SHARDING_KEY = 15;
    public static final byte SHARDING_PARTITION = 16;
    public static final byte TRACE_ID = 17;

    //判断标识
    public static final byte MESSAGESTART = 100;
    public static final byte PROPERTIS_START = 101;
    public static final byte IS_PROPERTY_FIELD = 102;

    //类型标识
    public static final byte INT = 1;
    public static final byte LONG = 2;
    public static final byte DOUBLE = 4;
    public static final byte STRING = 8;

    public static final Map<String, Byte> HEADERS = new HashMap<String, Byte>(20) {{
        put("MessageId", MESSAGE_ID);
        put("Topic", TOPIC);
        put("Queue", QUEUE);
        put("BornTimestamp", BORN_TIMESTAMP);
        put("BornHost", BORN_HOST);
        put("StoreTimestamp", STORE_TIMESTAMP);
        put("StoreHost", STORE_HOST);
        put("StartTime", START_TIME);
        put("StopTime", STOP_TIME);
        put("Timeout", TIMEOUT);
        put("Priority", PRIORITY);
        put("Reliability", RELIABILITY);
        put("SearchKey", SEARCH_KEY);
        put("ScheduleExpression", SCHEDULE_EXPRESSION);
        put("ShardingKey", SHARDING_KEY);
        put("ShardingPartition", SHARDING_PARTITION);
        put("TraceId", TRACE_ID);
    }};

}