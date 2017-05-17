package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.Set;

import static io.openmessaging.demo.CustomConstants.*;

/**
 * Created by JesonLee
 * OutPut和文件是一对一的关系
 * on 2017/5/5.
 */
public class Output {
    private final RandomAccessFile memoryMappedFile;
    private MappedByteBuffer writer;
    private int mappedSize;
    private static final int DEFAULT_MAPPEDSIZE = 16 * 1024 * 1024;
    private long start = 0;

    public Output(String fileName) throws IOException {
        memoryMappedFile = new RandomAccessFile(fileName, "rw");
        mappedSize = DEFAULT_MAPPEDSIZE;
        writer = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, mappedSize);
    }

    public Output(String fileName, int mappedSize) throws IOException {
        memoryMappedFile = new RandomAccessFile(fileName, "rw");
        this.mappedSize = mappedSize;
        writer = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, this.mappedSize);
    }

    public void writeMessage(BytesMessage bytesMessage) {
        if (writer.position() >= (mappedSize - 1024)) {
            try {
                reMap();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        writer.put(MESSAGESTART);

        //序列化body的长度和主体
        byte[] data = bytesMessage.getBody();
        intToBytes(data.length);
        writer.put(data);

        //对Headers进行序列化
        KeyValue headers = bytesMessage.headers();
        Set<String> set = headers.keySet();
        for (String s : set) {
            Byte b = HEADERS.get(s);
            writer.put(b);
            switch (b) {
                //针对不同的键，采用不同的序列化策略
                case TOPIC:
                    stringToBytes(headers.getString(s));
                    break;
                case QUEUE:
                    stringToBytes(headers.getString(s));
                    break;
                case MESSAGE_ID:
                    stringToBytes(headers.getString(s));
                    break;
                case BORN_TIMESTAMP:
                    longToBytes(headers.getLong(s));
                    break;
                case BORN_HOST:
                    stringToBytes(headers.getString(s));
                    break;
                case STORE_TIMESTAMP:
                    longToBytes(headers.getLong(s));
                    break;
                case STORE_HOST:
                    stringToBytes(headers.getString(s));
                    break;
                case START_TIME:
                    longToBytes(headers.getLong(s));
                    break;
                case STOP_TIME:
                    longToBytes(headers.getLong(s));
                    break;
                case TIMEOUT:
                    intToBytes(headers.getInt(s));
                    break;
                case PRIORITY:
                    intToBytes(headers.getInt(s));
                    break;
                case RELIABILITY:
                    intToBytes(headers.getInt(s));
                    break;
                case SEARCH_KEY:
                    stringToBytes(headers.getString(s));
                    break;
                case SCHEDULE_EXPRESSION:
                    stringToBytes(headers.getString(s));
                    break;
                case SHARDING_KEY:
                    stringToBytes(headers.getString(s));
                    break;
                case SHARDING_PARTITION:
                    stringToBytes(headers.getString(s));
                    break;
                case TRACE_ID:
                    stringToBytes(headers.getString(s));
                    break;

            }
        }

        //标记为PROPERTIS_START
        DefaultKeyValue properties = (DefaultKeyValue) bytesMessage.properties();
        writer.put(PROPERTIS_START);
        //序列化其中的map
        Map<String, Object> map = properties.getMap();
        mapToBytes(map);
    }

    public void reMap() throws IOException {
        int position = writer.position();
        start = start + position;
        this.writer = memoryMappedFile.getChannel().map
                (FileChannel.MapMode.READ_WRITE,
                        start,
                        mappedSize);
    }


    private void intToBytes(int value) {
        //由高位到低位
        writer.put((byte) (value >>> 24));
        writer.put((byte) (value >>> 16));
        writer.put((byte) (value >>> 8));
        writer.put((byte) value);
    }


    private void longToBytes(long num) {
        writer.put((byte) (num >>> 56));// 取最高8位放到0下标
        writer.put((byte) (num >>> 48));// 取最高8位放到0下标
        writer.put((byte) (num >>> 40));// 取最高8位放到0下标
        writer.put((byte) (num >>> 32));// 取最高8位放到0下标
        writer.put((byte) (num >>> 24));// 取最高8位放到0下标
        writer.put((byte) (num >>> 16));// 取次高8为放到1下标
        writer.put((byte) (num >>> 8)); // 取次低8位放到2下标
        writer.put((byte) (num)); // 取最低8位放到3下标
    }

    private void doubleToBytes(double num) {
        longToBytes(Double.doubleToLongBits(num));
    }


    private void stringToBytes(String s) {
        intToBytes(s.length());
        writer.put(s.getBytes());
    }

    private void mapToBytes(Map<String, Object> map) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            //说明这是属性
            writer.put(IS_PROPERTY_FIELD);

            String fieldName = entry.getKey();
            //写入键
            stringToBytes(fieldName);
            Object fieldValue = entry.getValue();
            if (fieldValue instanceof Integer) {
                int i = (int) fieldValue;
                writer.put(INT);
                intToBytes(i);
            } else if (fieldValue instanceof Long) {
                long l = (long) fieldValue;
                writer.put(LONG);
                longToBytes(l);
            } else if (fieldValue instanceof Double) {
                double d = (double) fieldValue;
                writer.put(DOUBLE);
                doubleToBytes(d);
            } else {
                String s = (String) fieldValue;
                writer.put(STRING);
                stringToBytes(s);
            }
        }
    }
}
