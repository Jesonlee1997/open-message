package io.openmessaging.demo.serialize;

import io.openmessaging.BytesMessage;
import io.openmessaging.Message;
import io.openmessaging.MessageFactory;
import io.openmessaging.MessageHeader;
import io.openmessaging.demo.DefaultMessageFactory;
import io.openmessaging.demo.MessageReader;
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

    private String fileName = STORE_PATH + "/" + "test";
    private MessageFactory messageFactory = new DefaultMessageFactory();
    private int messageNumber = 100000;


    @Test //测试Mmap序列化
    public void testOutput() throws IOException {

        Output output = new Output(fileName, 10 * 1024 * 1024);
        BytesMessage[] messages = new BytesMessage[messageNumber];
        for (int i = 0; i < messageNumber; i++) {
            BytesMessage message = messageFactory.createBytesMessageToTopic("topic", "sfsdfa".getBytes());
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

    @Test//测试原生序列化
    public void testOutPut2() throws IOException {
        File file = new File(fileName);
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();
        ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(file));
        BytesMessage[] messages = new BytesMessage[messageNumber];
        for (int i = 0; i < messageNumber; i++) {
            BytesMessage message = messageFactory.createBytesMessageToTopic("topic", "Preswfr_231".getBytes());
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
        MessageReader messageReader = new MessageReader(STORE_PATH + "/" + "QUEUE_1");
        List<Message> messages = new ArrayList<>(messageNumber);
        while (true) {
            Message message = messageReader.readMessage();
            if (message == null) {
                break;
            }
            messages.add(message);
        }
        messageReader.close();
        long end = System.currentTimeMillis();
        System.out.println(end - start);
        System.out.println("读取的消息数：" + messages.size());
    }

    @Test
    public void test2() throws IOException {

        String file = STORE_PATH+"QUEUE_0";
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        MappedByteBuffer mappedByteBuffer;

        long start = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            mappedByteBuffer = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_ONLY, (i)*1024*1024, (i+1) * 1024 * 1024);

        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);
        //MappedByteBuffer mappedByteBuffer = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, randomAccessFile.length()/2 + 1);
        //MappedByteBuffer mappedByteBuffer1 = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, randomAccessFile.length()/2 + 1, randomAccessFile.length() + 1);
        System.out.println();
    }
}
