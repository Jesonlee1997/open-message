package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.Message;

import java.io.IOException;

import static io.openmessaging.tester.Constants.STORE_PATH;

/**
 * 一个线程对应一个缓冲区
 * Created by JesonLee
 * on 2017/4/21.
 */
public class Buffer {
    private final Output output;
    public void add(Message message) throws IOException {

        synchronized (output) {
            output.writeMessage((BytesMessage) message);
        }

    }


    public Buffer(String bucket) throws IOException {
        output = new Output(STORE_PATH+"/"+bucket);
    }
}
