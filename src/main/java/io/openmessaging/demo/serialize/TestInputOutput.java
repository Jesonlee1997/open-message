package io.openmessaging.demo.serialize;

import io.openmessaging.BytesMessage;
import io.openmessaging.Message;
import io.openmessaging.MessageFactory;
import io.openmessaging.MessageHeader;
import io.openmessaging.demo.DefaultMessageFactory;
import io.openmessaging.demo.Input;
import io.openmessaging.demo.Output;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by JesonLee
 * on 2017/5/5.
 */
public class TestInputOutput {

    private String fileName = "J:\\Github\\openmessagingdemotester\\src\\main\\java\\io\\openmessaging\\demo\\serialize\\largeFile.txt";
    private MessageFactory messageFactory = new DefaultMessageFactory();
    private int messageNumber = 100000;

    @Test //测试序列化
    public void testOutput() throws IOException {
        long start = System.currentTimeMillis();
        Output output = new Output(fileName, messageNumber * 200);
        for (int i = 0; i < messageNumber; i++) {
            BytesMessage message = messageFactory.createBytesMessageToQueue("queue", "sfsdfa".getBytes());
            message.putHeaders(MessageHeader.MESSAGE_ID, 13424321L);
            message.putProperties("pro1--"+i, 1331);
            message.putProperties("pro2--"+i, 1324.31);
            message.putProperties("pro3--"+i, "sdfhkajshdf");
            output.writeMessage(message);
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }

    @Test //测试反序列化
    public void testInput() throws IOException {
        long start = System.currentTimeMillis();
        Input input = new Input(fileName, messageNumber * 200);
        List<Message> messages = new ArrayList<>(100000);
        while (true) {
            Message message = input.readMessage();
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
}
