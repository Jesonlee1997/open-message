package io.openmessaging.demo;

import io.openmessaging.MessageHeader;

import java.util.Map;

/**
 * Created by JesonLee
 * on 2017/5/2.
 */
public class Util {
    public static byte[] propertiesToBytes(DefaultKeyValue keyValue) {
        Map<String, Object> map = keyValue.getMap();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.equals(MessageHeader.TOPIC)) {

            }
        }
        return null;
        //TODO
    }
}
