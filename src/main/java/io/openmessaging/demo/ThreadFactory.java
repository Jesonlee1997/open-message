package io.openmessaging.demo;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by JesonLee
 * on 2017/4/19.
 */
public class ThreadFactory {
    public static int producerCount = 0;
    public AtomicInteger producerRound = new AtomicInteger(0);
    public AtomicInteger consumerRound = new AtomicInteger(0);

    public static String[] topics = new String[]{
            "TOPIC_0", "TOPIC_1", "TOPIC_2", "TOPIC_3", "TOPIC_4", "TOPIC_5", "TOPIC_6", "TOPIC_7", "TOPIC_8", "TOPIC_9"
    };
    public static String[] queues = new String[]{
            "QUEUE_0", "QUEUE_1", "QUEUE_2", "QUEUE_3", "QUEUE_4", "QUEUE_5", "QUEUE_6", "QUEUE_7", "QUEUE_8", "QUEUE_9"
    };
}
