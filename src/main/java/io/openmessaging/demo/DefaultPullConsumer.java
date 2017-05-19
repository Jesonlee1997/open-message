package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;

import java.util.*;

public class DefaultPullConsumer implements PullConsumer {
    private MessageStoreClient messageStore = MessageStoreClient.getInstance();
    private KeyValue properties;
    private String queue;
    private List<String> bucketList = new ArrayList<>();

    private int lastIndex = 0;

    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
        String storePath = properties.getString("STORE_PATH");
        messageStore.initPath(storePath);
    }


    @Override
    public KeyValue properties() {
        return properties;
    }

    @Override
    public Message poll() {
        if (bucketList.size() == 0 || queue == null) {
            return null;
        }
        //use Round Robin
        int checkNum = 0;
        while (++checkNum <= bucketList.size()) {
            //轮询获得topic或List
            String bucket = bucketList.get((++lastIndex) % (bucketList.size()));
            Message message = messageStore.pullMessage(bucket);
            if (message != null) {
                return message;
            } else {
                bucketList.remove(bucket);
                checkNum--;
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
        bucketList.add(queue);
        if (topics != null) {
            bucketList.addAll(topics);
        }

    }
}
