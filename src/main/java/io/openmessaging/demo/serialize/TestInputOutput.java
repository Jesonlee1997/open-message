package io.openmessaging.demo.serialize;

import io.openmessaging.BytesMessage;
import io.openmessaging.Message;
import io.openmessaging.MessageFactory;
import io.openmessaging.MessageHeader;
import io.openmessaging.demo.DefaultMessageFactory;
import io.openmessaging.demo.Input;
import io.openmessaging.demo.Output;
import org.junit.Test;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import static io.openmessaging.tester.Constants.STORE_PATH;

/**
 * Created by JesonLee
 * on 2017/5/5.
 */
public class TestInputOutput {

    private String fileName = "J:\\Github\\openmessagingdemotester\\src\\main\\java\\io\\openmessaging\\demo\\serialize\\largeFile.txt";
    private MessageFactory messageFactory = new DefaultMessageFactory();
    private int messageNumber = 10000;


    @Test //测试序列化
    public void testOutput() throws IOException {

        Output output = new Output(fileName, messageNumber * 100);
        BytesMessage[] messages = new BytesMessage[messageNumber];
        for (int i = 0; i < messageNumber; i++) {
            BytesMessage message = messageFactory.createBytesMessageToQueue("queue", "sfsdfa".getBytes());
            message.putHeaders(MessageHeader.MESSAGE_ID, 13424321L);
            message.putProperties("pro1--"+i, 1331);
            message.putProperties("pro2--"+i, 1324.31);
            message.putProperties("pro3--"+i, "sdfhkajshdf");
            messages[i] = message;
        }
        long start = System.currentTimeMillis();

        for (BytesMessage message : messages) {
            output.writeMessage(message);
        }

        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }

    @Test
    public void testOutPut2() throws IOException {
        File file = new File(fileName);
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();
        ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(file));
        BytesMessage[] messages = new BytesMessage[messageNumber];
        for (int i = 0; i < messageNumber; i++) {
            BytesMessage message = messageFactory.createBytesMessageToQueue("queue", "sfsdfa".getBytes());
            message.putHeaders(MessageHeader.MESSAGE_ID, 13424321L);
            message.putProperties("pro1--"+i, 1331);
            message.putProperties("pro2--"+i, 1324.31);
            message.putProperties("pro3--"+i, "sdfhkajshdf");
            messages[i] = message;
        }
        long start = System.currentTimeMillis();

        for (BytesMessage message : messages) {
            outputStream.writeObject(message);
        }

        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }

    @Test //测试反序列化
    public void testInput() throws IOException {
        long start = System.currentTimeMillis();
        Input input = new Input(STORE_PATH + "TOPIC_4");
        List<Message> messages = new ArrayList<>(messageNumber);
        while (true) {
            Message message = input.readMessage(0);
            if (message == null) {
                break;
            }
            messages.add(message);
        }
        input.close();
        long end = System.currentTimeMillis();
        System.out.println(end - start);
        System.out.println("读取的消息数：" + messages.size());
    }

    @Test
    public void test2() throws IOException {
        String file = STORE_PATH+"QUEUE_0";
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        MappedByteBuffer mappedByteBuffer = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, randomAccessFile.length()/2 + 1);
        MappedByteBuffer mappedByteBuffer1 = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, randomAccessFile.length()/2 + 1, randomAccessFile.length() + 1);
        System.out.println();
    }
}
