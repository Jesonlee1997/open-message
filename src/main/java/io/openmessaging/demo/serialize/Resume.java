package io.openmessaging.demo.serialize;

import io.openmessaging.Message;

import java.util.Map;

/**
 * Created by JesonLee
 * on 2017/5/4.
 */
public class Resume {
    private static byte[] bytes = new byte[100];
    private static int position = 0;

    public static long bytesToLong1(byte[] data) {
        return (long)data[0] << 56 //
                | (long)(data[1] & 0xFF) << 48 //
                | (long)(data[2] & 0xFF) << 40 //
                | (long)(data[3] & 0xFF) << 32 //
                | (long)(data[4] & 0xFF) << 24 //
                | (data[5] & 0xFF) << 16 //
                | (data[6] & 0xFF) << 8 //
                | data[7] & 0xFF;
    }

    public static long bytesToLong() {
        return (long)bytes[position++] << 56 //
                | (long)(bytes[position++] & 0xFF) << 48 //
                | (long)(bytes[position++] & 0xFF) << 40 //
                | (long)(bytes[position++] & 0xFF) << 32 //
                | (long)(bytes[position++] & 0xFF) << 24 //
                | (bytes[position++] & 0xFF) << 16 //
                | (bytes[position++] & 0xFF) << 8 //
                | bytes[position++] & 0xFF;
    }

    public static Message bytesToMessage() throws Exception {
        return null;
    }

    public static double bytesToDouble(byte[] data) {
        return Double.longBitsToDouble(bytesToLong1(data));
    }

    public static int bytesToInt() {
        return (bytes[position++] & 0xFF) << 24 //
                | (bytes[position++] & 0xFF) << 16 //
                | (bytes[position++] & 0xFF) << 8 //
                | bytes[position++] & 0xFF;
    }

    public static Map<String, Object> bytesToMap() {
        return null;
    }

}
