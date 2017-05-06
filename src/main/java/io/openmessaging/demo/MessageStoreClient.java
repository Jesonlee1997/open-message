package io.openmessaging.demo;

import io.openmessaging.Message;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.openmessaging.tester.Constants.STORE_PATH;

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
    private final String basePath = STORE_PATH;

    //存放bucket到消息列表的映射
    private final Map<String, BucketMessages> store = new ConcurrentHashMap<>();


    //用queue来标识线程
    public Message pullMessage(String queue, String bucket) {
        BucketMessages bucketMessages = store.get(bucket);
        return bucketMessages.pullMessage(Thread.currentThread());
    }

}
