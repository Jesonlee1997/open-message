package io.openmessaging.demo;

import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import io.openmessaging.tester.Constants;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by JesonLee
 * on 2017/4/17.
 */
public class MessageStore {
    private static final MessageStore INSTANCE = new MessageStore();

    private static final String basePath =
            Constants.STORE_PATH;

    //TODO:使用策略，在内存中维护一个容量为500的buffer，每当buffer满了之后，就向磁盘写入消息
    private final Map<String, Map<Thread, Buffer>> bucketMaps = new HashMap<>();


    public static MessageStore getInstance() {
        INSTANCE.deleteAll(new File(basePath));
        INSTANCE.init();
        return INSTANCE;
    }

    public void init() {
        //TODO:为每个Topic或QUEUE创建目录
        String[] topics = ThreadFactory.topics;
        String[] queues = ThreadFactory.queues;
        for (String topic : topics) {
            String path = basePath + "\\" + topic;
            File file = new File(path);
            file.mkdirs();
        }
        for (String queue : queues) {
            String path = basePath + "\\" + queue;
            File file = new File(path);
            file.mkdirs();
        }
    }


    //删除目录下的所有文件
    public void deleteAll(File file) {
        if (file.isDirectory()) {
            String[] children = file.list();
            if (children == null) {
                return;
            }
            for (String child : children) {
                File file1 = new File(file, child);
                deleteAll(file1);
            }
        } else {
            file.delete();
        }
    }


    public void putMessage(Message message) {
        String bucket = getBucket(message);
        if (!bucketMaps.containsKey(bucket)) {
            synchronized (bucketMaps) {
                if (!bucketMaps.containsKey(bucket)) {
                    bucketMaps.put(bucket, new HashMap<>());
                }
            }
        }
        //获得当前线程对应的MessageList
        try {
            addMessageToBucket(bucket, message);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //经message放入相应的bucket，获得线程对应bucket的文件句柄
    //TODO；定期进行ObjectOutputStream的reset
    private void addMessageToBucket(String bucket, Message message) throws IOException {
        Thread thread = Thread.currentThread();

        //获得线程的缓冲区
        Map<Thread, Buffer> threadOutputs = bucketMaps.get(bucket);
        Buffer buffer = threadOutputs.get(thread);
        if (buffer == null) {
            File file = new File(basePath + "\\" + bucket + "\\" + thread.getName());
            synchronized (threadOutputs) {
                if (buffer == null) {
                    buffer = new Buffer(file);
                    threadOutputs.put(thread, buffer);
                }
            }
        }
        buffer.add(message);
    }

    private String getBucket(Message message) {
        String topic = message.headers().getString(MessageHeader.TOPIC);
        String queue = message.headers().getString(MessageHeader.QUEUE);
        return topic != null ? topic : queue;
    }
}
