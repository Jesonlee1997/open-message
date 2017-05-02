package io.openmessaging.demo.test;

import io.openmessaging.BytesMessage;
import io.openmessaging.Message;
import io.openmessaging.MessageFactory;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by JesonLee
 * on 2017/4/20.
 */
public class TestSerialize {
    private static MessageFactory messageFactory = new DefaultMessageFactory();

    private static String basePath = "J:\\Github\\openmessagingdemotester\\src\\main\\java\\io\\openmessaging\\demo\\test\\";

    @Test
    //测试一次写入含有500个数据的List的效率高还是一次一次写入1个Message的效率高
    //测试反序列化的效率那个高
    public void testPerformance1() throws IOException, ClassNotFoundException {
        Message[] messages = new Message[50000];
        for (int i = 0; i < messages.length; i++) {
            messages[i] = messageFactory.createBytesMessageToQueue("queueq", ("message" +i).getBytes());
        }

        long start = System.currentTimeMillis();
        List<Message> messageList = new ArrayList<>(Arrays.asList(messages));
        File file =
                new File(basePath +"test1.txt");
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
        objectOutputStream.writeObject(messageList);

        /*FileInputStream fileInputStream = new FileInputStream(file);
        ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
        List<Message> list = (List<Message>) objectInputStream.readObject();*/
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }

    @Test
    //测试一次写入含有500个数据的List的效率高还是一次一次写入1个Message的效率高
    public void testPerformance2() throws IOException, ClassNotFoundException {
        Message[] messages = new Message[50000];
        for (int i = 0; i < messages.length; i++) {
            messages[i] = messageFactory.createBytesMessageToQueue("queueq", ("message" +i).getBytes());
        }
        long start = System.currentTimeMillis();
        File file = new File(basePath + "test2.txt");
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        for (Message message : messages) {
            fileOutputStream.write("testwqeqwqeas".getBytes());
        }
        /*List<Message> list = new ArrayList<>(50000);
        FileInputStream fileInputStream = new FileInputStream(file);
        ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
        try {
            while (true) {
                list.add((Message) objectInputStream.readObject());
            }
        } catch (EOFException e) {
        }
*/
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }


    //使用原生数组进行序列化
    @Test
    public void testPerformance3() throws IOException {
        Message[] messages = new Message[50000];
        for (int i = 0; i < messages.length; i++) {
            messages[i] = messageFactory.createBytesMessageToQueue("queueq", ("message" +i).getBytes());
        }
        long start = System.currentTimeMillis();
        File file = new File(basePath + "test3.txt");
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
        objectOutputStream.writeObject(messages);

        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }

    public void testByteWrite() throws IOException {
        BytesMessage[] messages = new BytesMessage[50000];
        for (int i = 0; i < messages.length; i++) {
            messages[i] = messageFactory.createBytesMessageToQueue("queueq", ("message" +i).getBytes());
        }
        long start = System.currentTimeMillis();
        File file = new File(basePath + "testByteWrite.txt");
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        for (BytesMessage message : messages) {
            fileOutputStream.write(message.getBody());
            message.properties();
        }

        long end = System.currentTimeMillis();
        System.out.println(end - start);

    }

    @Test
    public void testWrite1() throws IOException {
        String[] strings = new String[50000];
        for (int i = 0; i < strings.length; i++) {
            strings[i] = "testWrite1" + i;
        }
        File file = new File(basePath + "testWrite.txt");
        long start = System.currentTimeMillis();
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        for (String string : strings) {
            fileOutputStream.write(string.getBytes());
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }
}


