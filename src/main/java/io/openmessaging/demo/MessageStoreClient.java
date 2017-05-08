package io.openmessaging.demo;

import io.openmessaging.Message;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by JesonLee
 * on 2017/4/20.
 */
public class MessageStoreClient {
    private final static MessageStoreClient INSTANCE = new MessageStoreClient();

    public static MessageStoreClient getInstance() {
        return INSTANCE;
    }

    //存放bucket到消息列表的映射
    private final Map<String, MessageReader> store = new ConcurrentHashMap<>();


    //用queue来标识线程
    public Message pullMessage(String bucket) {
        MessageReader messageReader = store.get(bucket);
        if (messageReader == null) {
            synchronized (store) {
                if (store.get(bucket) == null) {
                    messageReader = new MessageReader(bucket);
                    store.put(bucket, messageReader);
                }
            }
        }
        return messageReader.readMessage();
    }

}
