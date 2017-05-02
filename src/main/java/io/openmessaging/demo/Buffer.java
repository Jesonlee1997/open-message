package io.openmessaging.demo;

import io.openmessaging.Message;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * 一个线程对应一个缓冲区
 * Created by JesonLee
 * on 2017/4/21.
 */
public class Buffer {
    private final int limit = 50;
    private Message[] messages = new Message[limit];
    private int index = 0;
    private ObjectOutputStream objectOutputStream;
    public void add(Message message) throws IOException {
        messages[index++] = message;
        //objectOutputStream.writeObject(message);
        if (index >= limit)
            serializeAndClear();
    }

    //将消息序列化到磁盘上
    private void serializeAndClear() throws IOException {
        objectOutputStream.writeObject(messages);
        messages = new Message[limit];
        /*for (int i = 0; i < messages.length; i++) {
            //objectOutputStream.writeObject(messages[i]);
            messages[i] = null;
        }*/
        index = 0;
    }

    Buffer(File file) throws IOException {
        this.objectOutputStream = new ObjectOutputStream(new FileOutputStream(file));
    }
}
