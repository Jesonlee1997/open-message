package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.MessageFactory;
import io.openmessaging.MessageHeader;

/**
 * Created by JesonLee
 * on 2017/4/20.
 */
public class DefaultMessageFactory implements MessageFactory {

    @Override public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {
        DefaultBytesMessage defaultBytesMessage = new DefaultBytesMessage(body);
        defaultBytesMessage.putHeaders(MessageHeader.TOPIC, topic);
        return defaultBytesMessage;
    }

    @Override public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {
        DefaultBytesMessage defaultBytesMessage = new DefaultBytesMessage(body);
        defaultBytesMessage.putHeaders(MessageHeader.QUEUE, queue);
        return defaultBytesMessage;
    }
}
