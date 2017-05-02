package io.openmessaging.demo;

import io.openmessaging.Message;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by JesonLee
 * on 2017/4/20.
 */
public class MessageStoreClient {
    private final static MessageStoreClient INSTANCE = new MessageStoreClient();

    public static MessageStoreClient getInstance() {
        return INSTANCE;
    }

    //
    private final String basePath =
            "J:\\Github\\open-messaging-demo\\src\\main\\java\\io\\openmessaging\\myImpl1\\store";


    //存放bucket到消息列表的映射
    private final Map<String, BucketMessages> store = new HashMap<>();

    //TODO:从磁盘中读取对应bucket的信息
    public void load(String bucket) throws IOException, ClassNotFoundException {
        BucketMessages messages = store.get(bucket);
        if (messages == null) {
            //将
            messages = new BucketMessages();
            store.put(bucket, messages);

            //topic或者queue的路径
            String path = basePath + "\\" + bucket;

        }
    }

    //用queue来标识线程
    public Message pullMessage(String queue, String bucket) {
        BucketMessages bucketMessages = store.get(bucket);
        return bucketMessages.pullMessage(queue);
    }

    public void addBucket(String bucket) {
        if (!store.containsKey(bucket)) {
            synchronized (store) {
                if (!store.containsKey(bucket)) {
                    store.put(bucket, new BucketMessages());
                }
            }
            store.values();
        }


    }

    public void addBuckets(List<String> topics) {
        for (String topic : topics) {
            addBucket(topic);
        }
    }

}
