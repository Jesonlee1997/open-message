package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.Message;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.tester.Constants.STORE_PATH;

/**
 * 一个线程对应一个缓冲区
 * Created by JesonLee
 * on 2017/4/21.
 */
public class Buffer {
    private static final int MAPPED_SIZE = 1024;
    private AtomicInteger position = new AtomicInteger(0);
    private int bufferSize = 500;
    private final BytesMessage[] messageBuffer = new BytesMessage[bufferSize];
    Queue<Message> queue = new ConcurrentLinkedQueue<>();
    private Output output;
    private final byte[] lock = new byte[0];
    public void add(Message message) throws IOException {
        /*if (position.incrementAndGet() >= bufferSize) {
            synchronized (lock) {
                if (position.get() >= bufferSize) {
                    //TODO:能否优化？
                    for (int i = 0; i < bufferSize; i++) {
                        output.writeMessage(messageBuffer[i]);
                    }
                    position.set(1);
                }
            }
        }
        messageBuffer[position.get()-1] = (BytesMessage) message;*/

        synchronized (this) {
            output.writeMessage((BytesMessage) message);
        }
        //messageBuffer[position.getAndIncrement()] = (BytesMessage) message;

    }


    public Buffer(String bucket) throws IOException {
        output = new Output(STORE_PATH+bucket);
    }
}
