package io.openmessaging.demo;

import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import io.openmessaging.PullConsumer;

import java.util.List;

/**
 * Created by JesonLee
 * on 2017/4/24.
 */
public class ConsumerThread extends Thread {

    private PullConsumer pullConsumer;

    @Override
    public void run() {
        while (true) {
            Message message = pullConsumer.poll();
            if (message == null) {
                System.out.println("消息已读完");
                break;
            }
        }
    }

    public ConsumerThread(String queue, List<String> topics) {
        pullConsumer = new DefaultPullConsumer(pullConsumer.properties());
        pullConsumer.attachQueue(queue, topics);
    }

    private String getBucket(Message message) {
        String topic = message.headers().getString(MessageHeader.TOPIC);
        String queue = message.headers().getString(MessageHeader.QUEUE);
        return topic != null ? topic : queue;
    }
}
