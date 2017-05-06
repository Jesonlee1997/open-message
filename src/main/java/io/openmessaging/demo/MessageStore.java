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

    //TODO:使用策略，在内存中维护一个容量为500的buffer，每当buffer满了之后，就向磁盘写入消息
    private final Map<String, Buffer> bucketMaps = new ConcurrentHashMap<>();

    public static MessageStore getInstance() {
        return INSTANCE;
    }

    public void putMessage(Message message) throws IOException {
        String bucket = getBucket(message);
        Buffer buffer = bucketMaps.get(bucket);
        if (buffer == null) {//TODO:线程安全？
            //不管这个bucket是否存在，Buffer都会创建一个文件mmap到内存中
            buffer = new Buffer(bucket);
            bucketMaps.put(bucket, buffer);
        }
        buffer.add(message);
    }

    private String getBucket(Message message) {
        String topic = message.headers().getString(MessageHeader.TOPIC);
        String queue = message.headers().getString(MessageHeader.QUEUE);
        return topic != null ? topic : queue;
    }
}
