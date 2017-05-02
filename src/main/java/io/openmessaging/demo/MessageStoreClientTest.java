package io.openmessaging.demo;

import org.junit.Test;

/**
 * Created by JesonLee
 * on 2017/4/20.
 */
public class MessageStoreClientTest {
    private MessageStoreClient messageStoreClient = MessageStoreClient.getInstance();

    @Test
    public void load() throws Exception {
        messageStoreClient.load("QUEUE1");
    }

}