package io.openmessaging.demo;

import io.openmessaging.*;

import java.io.IOException;

public class DefaultProducer  implements Producer {
    private MessageStore messageStore = MessageStore.getInstance();
    private KeyValue properties;

    public DefaultProducer(KeyValue properties) {
        this.properties = properties;
        String path = properties.getString("STORE_PATH");
        messageStore.initPath(path);
    }

    public DefaultProducer() {
    }

    @Override
    public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {
        BytesMessage message = new DefaultBytesMessage(body);
        message.putHeaders(MessageHeader.TOPIC, topic);
        return message;
    }

    @Override
    public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {
        BytesMessage message = new DefaultBytesMessage(body);
        message.putHeaders(MessageHeader.QUEUE, queue);
        return message;
    }

    @Override
    public void start() {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public KeyValue properties() {
        return null;
    }

    @Override
    public void send(Message message) {
        try {
            messageStore.putMessage(message);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void send(Message message, KeyValue properties) {

    }

    @Override
    public Promise<Void> sendAsync(Message message) {
        return null;
    }

    @Override
    public Promise<Void> sendAsync(Message message, KeyValue properties) {
        return null;
    }

    @Override
    public void sendOneway(Message message) {

    }

    @Override
    public void sendOneway(Message message, KeyValue properties) {

    }

    @Override
    public BatchToPartition createBatchToPartition(String partitionName) {
        return null;
    }

    @Override
    public BatchToPartition createBatchToPartition(String partitionName, KeyValue properties) {
        return null;
    }
}
