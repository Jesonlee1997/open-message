package io.openmessaging.demo.test;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by JesonLee
 * on 2017/5/6.
 */
public class TestMap {


    @Test
    public void testMap() {


        long start = System.currentTimeMillis();

        Map<String, String> map = new HashMap<>(110000);
        for (int i = 0; i < 110000; i++) {
            map.put("key"+i, "kdshfesda"+i);
        }

        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }

    @Test
    public void testCocuMap() {
        long start = System.currentTimeMillis();

        ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>(110000);
        for (int i = 0; i < 110000; i++) {
            map.put("key"+i, "kdshfesda"+i);
        }

        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }
}
