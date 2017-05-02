package io.openmessaging.demo;

import org.junit.Test;

/**
 * Created by JesonLee
 * on 2017/4/19.
 */
public class MessageStoreTest {
    MessageStore messageStore = MessageStore.getInstance();
    @Test
    public void init() throws Exception {
        messageStore.init();
    }

    @Test
    public void deleteAllDir() {
    }

}