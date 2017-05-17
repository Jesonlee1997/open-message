package io.openmessaging.demo;

import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import io.openmessaging.tester.Constants;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by JesonLee
 * on 2017/4/17.
 */
public class MessageStore {
    private static final MessageStore INSTANCE = new MessageStore();

    private static final String basePath = Constants.STORE_PATH;

    //每个bucket都对应一个Buffer，Buffer利用mmap直接向程序中写入
    private final Map<String, Buffer> bucketMaps = new ConcurrentHashMap<>();

    public static MessageStore getInstance() {
        return INSTANCE;
    }

    public void putMessage(Message message) throws IOException {
        String bucket = getBucket(message);

        if (bucketMaps.get(bucket) == null) {
            //不管这个bucket是否存在，Buffer都会创建一个文件mmap到内存中
            synchronized (bucketMaps) {
                if (bucketMaps.get(bucket) == null) {
                    bucketMaps.put(bucket, new Buffer(bucket));
                }
            }
        }

        bucketMaps.get(bucket).add(message);
    }

    private String getBucket(Message message) {
        String topic = message.headers().getString(MessageHeader.TOPIC);
        String queue = message.headers().getString(MessageHeader.QUEUE);
        return topic != null ? topic : queue;
    }
}
