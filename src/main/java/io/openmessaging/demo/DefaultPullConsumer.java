package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;

import java.util.*;

public class DefaultPullConsumer implements PullConsumer {
    private MessageStoreClient messageStore = MessageStoreClient.getInstance();
    private KeyValue properties;
    private String queue;
    private Set<String> buckets = new HashSet<>();
    private List<String> bucketList = new ArrayList<>();

    private int lastIndex = 0;

    //TODO:添加属性
    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
    }


    @Override
    public KeyValue properties() {
        return properties;
    }

    @Override
    public Message poll() {
        if (buckets.size() == 0 || queue == null) {
            return null;
        }
        //use Round Robin
        int checkNum = 0;
        while (++checkNum <= bucketList.size()) {
            //轮询获得topic或List
            String bucket = bucketList.get((++lastIndex) % (bucketList.size()));
            Message message = messageStore.pullMessage(queue, bucket);
            if (message != null) {
                return message;
            }
        }
        return null;
    }

    @Override
    public Message poll(KeyValue properties) {
        return null;
    }

    @Override
    public void ack(String messageId) {

    }

    @Override
    public void ack(String messageId, KeyValue properties) {

    }

    @Override
    //将线程绑定到一个队列和多个topic
    public void attachQueue(String queueName, Collection<String> topics) {
        queue = queueName;
        buckets.addAll(topics);
        bucketList.addAll(buckets);
    }
}
