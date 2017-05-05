package io.openmessaging.demo.serialize;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.MessageHeader;
import io.openmessaging.demo.DefaultKeyValue;
import io.openmessaging.demo.DefaultMessageFactory;
import org.junit.Test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.openmessaging.demo.serialize.Resume.bytesToDouble;
import static io.openmessaging.demo.serialize.Resume.bytesToLong1;

/**
 * Created by JesonLee
 * on 2017/5/3.
 */
public class Consts {
    private static final int LENGTH = 4;//记录bytes的长度，此处为4位

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

    public static final Map<String, Byte> HEADERS = new HashMap<String, Byte>(20) {{
        put("MessageId", MESSAGE_ID);
        put("Topic", TOPIC);
        put("Queue", QUEUE);
        /*put("BORN_TIMESTAMP", BORN_TIMESTAMP);
        put("BORN_HOST", BORN_HOST);
        put("STORE_TIMESTAMP", STORE_TIMESTAMP);
        put("STORE_HOST", STORE_HOST);
        put("START_TIME", START_TIME);
        put("STOP_TIME", STOP_TIME);
        put("TIMEOUT", TIMEOUT);
        put("PRIORITY", PRIORITY);
        put("RELIABILITY", RELIABILITY);
        put("SEARCH_KEY", SEARCH_KEY);
        put("SCHEDULE_EXPRESSION", SCHEDULE_EXPRESSION);
        put("SHARDING_KEY", SHARDING_KEY);
        put("SHARDING_PARTITION", SHARDING_PARTITION);
        put("TRACE_ID", TRACE_ID);*/
    }};

    public static final int MESSAGESTART = 100;
    public static final int PROPERTIS_START = 101;

    public static final int INT = 1;
    public static final int LONG = 2;
    public static final int DOUBLE = 4;
    public static final int STRING = 8;


    public static int position;

    public static byte[] bytes = new byte[100];

    public static void require(int length) {
    }


    DefaultMessageFactory defaultMessageFactory = new DefaultMessageFactory();


    public static void messageToBytes(BytesMessage bytesMessage) {
        bytesMessage.putHeaders(MessageHeader.MESSAGE_ID, 12345235L);
        bytes[position++] = MESSAGESTART;

        //序列化body的长度和主体
        byte[] data = bytesMessage.getBody();
        intToBytes(data.length);
        System.arraycopy(data, 0, bytes, position, data.length);
        position += data.length;

        //对Headers进行序列化
        KeyValue headers = bytesMessage.headers();
        Set<String> set = headers.keySet();
        for (String s : set) {
            Byte b = HEADERS.get(s);
            bytes[position++] = b;
            switch (b) {
                //针对不同的键，采用不同的序列化策略
                case MESSAGE_ID:
                    longToBytes(headers.getLong(s));
                    break;
                case TOPIC:
                    stringToBytes(headers.getString(s));
                    break;
                case QUEUE:
                    stringToBytes(headers.getString(s));
                    break;
            }
        }

        //标记为PROPERTIS_START
        DefaultKeyValue properties = (DefaultKeyValue) bytesMessage.properties();
        bytes[position++] = PROPERTIS_START;

        //序列化其中的map
        Map<String, Object> map = properties.getMap();
        mapToBytes(map);
    }



    private static void intToBytes(int value) {
        //由高位到低位
        bytes[position++] = (byte)(value >>> 24);
        bytes[position++] = (byte)(value >>> 16);
        bytes[position++] = (byte)(value >>> 8);
        bytes[position++] = (byte)value;
    }


    private static byte[] intToBytes1(int value) {
        //由高位到低位
        byte[] bytes = new byte[4];
        bytes[0] = (byte)(value >>> 24);
        bytes[1] = (byte)(value >>> 16);
        bytes[2] = (byte)(value >>> 8);
        bytes[3] = (byte)value;
        return bytes;
    }

    private static void longToBytes(long num) {
        bytes[position++] = (byte) (num >>> 56);// 取最高8位放到0下标
        bytes[position++] = (byte) (num >>> 48);// 取最高8位放到0下标
        bytes[position++] = (byte) (num >>> 40);// 取最高8位放到0下标
        bytes[position++] = (byte) (num >>> 32);// 取最高8位放到0下标
        bytes[position++] = (byte) (num >>> 24);// 取最高8位放到0下标
        bytes[position++] = (byte) (num >>> 16);// 取次高8为放到1下标
        bytes[position++] = (byte) (num >>> 8); // 取次低8位放到2下标
        bytes[position++] = (byte) (num); // 取最低8位放到3下标
    }

    private static byte[] longToBytes1(long num) {
        byte[] bytes = new byte[8];

        bytes[0] = (byte) (num >>> 56);// 取最高8位放到0下标
        bytes[1] = (byte) (num >>> 48);// 取最高8位放到0下标
        bytes[2] = (byte) (num >>> 40);// 取最高8位放到0下标
        bytes[3] = (byte) (num >>> 32);// 取最高8位放到0下标
        bytes[4] = (byte) (num >>> 24);// 取最高8位放到0下标
        bytes[5] = (byte) (num >>> 16);// 取次高8为放到1下标
        bytes[6] = (byte) (num >>> 8); // 取次低8位放到2下标
        bytes[7] = (byte) (num); // 取最低8位放到3下标
        return bytes;
    }



    private static void doubleToBytes(double num) {
        longToBytes(Double.doubleToLongBits(num));
    }

    private static byte[] doubleToBytes1(double num) {
        return longToBytes1(Double.doubleToLongBits(num));
    }



    private static void stringToBytes(String s) {
        intToBytes(s.length());
        byte[] data = s.getBytes();
        System.arraycopy(data, 0, bytes, position, data.length);
        position += data.length;
    }

    private static void mapToBytes(Map<String, Object> map) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String fieldName = entry.getKey();
            //写入键
            stringToBytes(fieldName);
            Object fieldValue = entry.getValue();
            if (fieldValue instanceof Integer) {
                int i = (int) fieldValue;
                bytes[position++] = INT;
                intToBytes(i);
            } else if (fieldValue instanceof Long) {
                long l = (long) fieldValue;
                bytes[position++] = LONG;
                longToBytes(l);
            } else if (fieldValue instanceof Double) {
                double d = (double) fieldValue;
                bytes[position++] = DOUBLE;
                doubleToBytes(d);
            } else {
                String s = (String) fieldValue;
                bytes[position++] = STRING;
                stringToBytes(s);
            }
        }
    }



    @Test
    public void testInt() {
        int i = -1234;
        byte[] bytes = intToBytes1(i);
        /*int result = bytesToInt(bytes);
        System.out.println(result);*/
    }

    @Test
    public void testDouble() {
        double d = -12341.1234;
        byte[] bytes = doubleToBytes1(d);
        System.out.println(bytesToDouble(bytes));
    }

    @Test
    public void testLong() {
        long l = 1234534321242343123L;
        byte[] bytes = longToBytes1(l);
        System.out.println(bytesToLong1(bytes));
    }

    @Test
    public void testString() {
        String s = "12345";
        stringToBytes(s);
    }

    @Test
    public void test() throws IOException {
        RandomAccessFile memoryMappedFile = new RandomAccessFile(
                "J:\\Github\\openmessagingdemotester\\src\\main\\java\\io\\openmessaging\\demo\\serialize\\test", "rw");
        // Mapping a file into memory
        MappedByteBuffer out = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, bytes.length+1);
        System.out.println(out.capacity());

        BytesMessage bytesMessage = defaultMessageFactory.createBytesMessageToQueue("topisafdasdfadfc", "queue1".getBytes());
        messageToBytes(bytesMessage);
        out.put(bytes);

        for (int i = 0; i < bytes.length; i++) {
            System.out.println(out.get(i));
        }
        /*for (int i = 0; i < 10; i++) {
            BytesMessage bytesMessage = defaultMessageFactory.createBytesMessageToQueue("queue1", "queue1".getBytes());
            out.put(messageToBytes(bytesMessage));
        }*/

    }
}
