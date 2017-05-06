package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.Set;

import static io.openmessaging.demo.serialize.Constants.*;

/**
 * Created by JesonLee
 * on 2017/5/5.
 */
public class Output {
    private final RandomAccessFile memoryMappedFile;
    private MappedByteBuffer outPut;
    private int mappedSize;
    private static final int DEFAULT_MAPPEDSIZE = 16 * 1024 * 1024;

    public Output(String fileName) throws IOException {
        memoryMappedFile = new RandomAccessFile(fileName, "rw");
        mappedSize = DEFAULT_MAPPEDSIZE;
        outPut = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, mappedSize);
    }

    public Output(String fileName, int mappedSize) throws IOException {
        memoryMappedFile = new RandomAccessFile(fileName, "rw");
        this.mappedSize = mappedSize;
        outPut = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, this.mappedSize);
    }

    public void writeMessage(BytesMessage bytesMessage) {
        if (outPut.position() >= (mappedSize - 1024)) {
            try {
                reMap();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        outPut.put(MESSAGESTART);

        //序列化body的长度和主体
        byte[] data = bytesMessage.getBody();
        //outPut.putInt(data.length);
        intToBytes(data.length);
        outPut.put(data);

        //对Headers进行序列化
        KeyValue headers = bytesMessage.headers();
        Set<String> set = headers.keySet();
        for (String s : set) {
            Byte b = HEADERS.get(s);
            outPut.put(b);
            switch (b) {
                //针对不同的键，采用不同的序列化策略
                case TOPIC:
                    stringToBytes(headers.getString(s));
                    break;
                case QUEUE:
                    stringToBytes(headers.getString(s));
                    break;
                case MESSAGE_ID:
                    longToBytes(headers.getLong(s));
                    break;
                case BORN_TIMESTAMP:
                    longToBytes(headers.getLong(s));
                    break;
                case BORN_HOST:
                    longToBytes(headers.getLong(s));
                    break;
                case STORE_TIMESTAMP:
                    longToBytes(headers.getLong(s));
                    break;
                case STORE_HOST:
                    longToBytes(headers.getLong(s));
                    break;
                case START_TIME:
                    longToBytes(headers.getLong(s));
                    break;
                case STOP_TIME:
                    longToBytes(headers.getLong(s));
                    break;
                case TIMEOUT:
                    longToBytes(headers.getLong(s));
                    break;
                case PRIORITY:
                    longToBytes(headers.getLong(s));
                    break;
                case RELIABILITY:
                    longToBytes(headers.getLong(s));
                    break;
                case SEARCH_KEY:
                    longToBytes(headers.getLong(s));
                    break;
                case SCHEDULE_EXPRESSION:
                    longToBytes(headers.getLong(s));
                    break;
                case SHARDING_KEY:
                    longToBytes(headers.getLong(s));
                    break;
                case SHARDING_PARTITION:
                    longToBytes(headers.getLong(s));
                    break;
                case TRACE_ID:
                    longToBytes(headers.getLong(s));
                    break;

            }
        }

        //标记为PROPERTIS_START
        DefaultKeyValue properties = (DefaultKeyValue) bytesMessage.properties();
        outPut.put(PROPERTIS_START);
        //序列化其中的map
        Map<String, Object> map = properties.getMap();
        mapToBytes(map);
        outPut.force();
    }

    public void reMap() throws IOException {
        int position = outPut.position();
        this.outPut = memoryMappedFile.getChannel().map
                (FileChannel.MapMode.READ_WRITE,
                        position,
                        position + mappedSize);//TODO：映射区的大小
    }


    private void intToBytes(int value) {
        //由高位到低位
        outPut.put((byte) (value >>> 24));
        outPut.put((byte) (value >>> 16));
        outPut.put((byte) (value >>> 8));
        outPut.put((byte) value);
    }


    private void longToBytes(long num) {
        outPut.put((byte) (num >>> 56));// 取最高8位放到0下标
        outPut.put((byte) (num >>> 48));// 取最高8位放到0下标
        outPut.put((byte) (num >>> 40));// 取最高8位放到0下标
        outPut.put((byte) (num >>> 32));// 取最高8位放到0下标
        outPut.put((byte) (num >>> 24));// 取最高8位放到0下标
        outPut.put((byte) (num >>> 16));// 取次高8为放到1下标
        outPut.put((byte) (num >>> 8)); // 取次低8位放到2下标
        outPut.put((byte) (num)); // 取最低8位放到3下标
    }

    private void doubleToBytes(double num) {
        longToBytes(Double.doubleToLongBits(num));
    }


    private void stringToBytes(String s) {
        intToBytes(s.length());
        outPut.put(s.getBytes());
    }

    private void mapToBytes(Map<String, Object> map) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            //说明这是属性
            outPut.put(IS_PROPERTY_FIELD);

            String fieldName = entry.getKey();
            //写入键
            stringToBytes(fieldName);
            Object fieldValue = entry.getValue();
            if (fieldValue instanceof Integer) {
                int i = (int) fieldValue;
                outPut.put(INT);
                intToBytes(i);
            } else if (fieldValue instanceof Long) {
                long l = (long) fieldValue;
                outPut.put(LONG);
                longToBytes(l);
            } else if (fieldValue instanceof Double) {
                double d = (double) fieldValue;
                outPut.put(DOUBLE);
                doubleToBytes(d);
            } else {
                String s = (String) fieldValue;
                outPut.put(STRING);
                stringToBytes(s);
            }
        }
    }
}
