package io.openmessaging.demo;

import io.openmessaging.*;

public class DefaultProducer  implements Producer {
    private MessageStore messageStore = MessageStore.getInstance();

    //TODO:添加属性值
    public DefaultProducer(KeyValue properties) {
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
        messageStore.putMessage(message);
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
