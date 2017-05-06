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

    private static final int MESSAGE_COUNT = 100;

    @Test
    public void testMap() throws InterruptedException {


        long start = System.currentTimeMillis();
        Map<String, String> map = new HashMap<>(MESSAGE_COUNT);
        int threadNum = 10;
        Thread[] threads = new Thread[threadNum];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Task1(map, i));
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);
        System.out.println("map的大小："+map.size());
    }

    @Test
    public void testCocuMap() throws InterruptedException {
        long start = System.currentTimeMillis();

        ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>(MESSAGE_COUNT);
        int threadNum = 10;
        Thread[] threads = new Thread[threadNum];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Task2(map, i));
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }

        long end = System.currentTimeMillis();
        System.out.println(end - start);
        System.out.println("map的大小："+map.size());
    }

    static class Task1 implements Runnable {
        final Map<String, String> map;
        final int id;

        public Task1(Map<String, String> map, int id) {
            this.map = map;
            this.id = id;
        }

        @Override
        public void run() {
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                synchronized (map) {
                    map.put(id + "key" + i, "kdshfesda" + i);
                }
            }
        }
    }

    static class Task2 implements Runnable {

        final Map<String, String> map;
        final int id;

        public Task2(Map<String, String> map, int id) {
            this.map = map;
            this.id = id;
        }


        @Override
        public void run() {
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                map.put(id + "key" + i, "kdshfesda" + i);
            }
        }
    }
}
