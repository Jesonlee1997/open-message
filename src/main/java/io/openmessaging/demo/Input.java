package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static io.openmessaging.demo.serialize.Constants.*;

/**
 * Created by JesonLee
 * on 2017/5/5.
 */
public class Input {
    private int position;
    private RandomAccessFile memoryMappedFile;
    private MappedByteBuffer input;

    public Input(String fileName) throws IOException {
        memoryMappedFile = new RandomAccessFile(fileName, "rw");
        if (memoryMappedFile.length() > Integer.MAX_VALUE) {
            input = memoryMappedFile.getChannel().map(
                    FileChannel.MapMode.READ_ONLY,
                    0,
                    Integer.MAX_VALUE);
        } else {
            input = memoryMappedFile.getChannel().map(
                    FileChannel.MapMode.READ_ONLY,
                    0,
                    memoryMappedFile.length());
        }
    }

    private byte[] getBytes(int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = input.get(position++);
        }
        return bytes;
    }

    public Message readMessage(int position) {
        if (input.get(position) != MESSAGESTART) {
            return null;
        }

        //开始反序列化
        BytesMessage bytesMessage = new DefaultBytesMessage();
        byte messageNum = input.get(position++);
        int bodyLength = input.getInt(position);
        position += 4;


        byte[] bytes = new byte[bodyLength];
        for (int i = 0; i < bodyLength; i++) {
            bytes[i] = input.get(position++);
        }
        bytesMessage.setBody(bytes);

        byte headerNum;
        while ((headerNum = input.get(position++)) != PROPERTIS_START) {
            //TODO:添加更多的判断，出现频率最高的if-else放在最前面
            if (headerNum == TOPIC) {

                int length = input.get(position);
                position += 4;

                bytes = new byte[length];
                for (int i = 0; i < length; i++) {
                    bytes[i] = input.get(position++);
                }
                bytesMessage.setBody(bytes);

                String s = new String(bytes);
                bytesMessage.putHeaders(MessageHeader.TOPIC, s);
            } else if (headerNum == QUEUE) {

                int length = input.getInt(position);
                position += 4;

                bytes = new byte[length];
                for (int i = 0; i < length; i++) {
                    bytes[i] = input.get(position++);
                }
                bytesMessage.setBody(bytes);

                String s = new String(bytes);
                bytesMessage.putHeaders(MessageHeader.QUEUE, s);
            } else if (headerNum == MESSAGE_ID) {

                long messageId = input.getLong(position);
                position += 8;
                bytesMessage.putHeaders(MessageHeader.MESSAGE_ID, messageId);
            } else {
                System.out.println("未定义的消息头");//TODO:抛异常
            }
        }
        while (input.get(position) == IS_PROPERTY_FIELD) {
            int length = input.getInt(++position);
            position += 4;
            bytes = new byte[length];
            for (int i = 0; i < length; i++) {
                bytes[i] = input.get(position++);
            }
            String fieName = new String(bytes);
            byte type = input.get(position++);
            if (type == INT) {
                int value = input.getInt(position);
                position += 4;
                bytesMessage.putProperties(fieName, value);
            } else if (type == LONG) {
                long valueL = input.getLong(position);
                position += 8;
                bytesMessage.putProperties(fieName, valueL);
            } else if (type == DOUBLE) {
                double value = input.getDouble(position);
                position += 8;
                bytesMessage.putProperties(fieName, value);
            } else if (type == STRING) {
                int valueLength = input.getInt(position);
                position += 4;

                bytes = getBytes(valueLength);
                String valueS = new String(bytes);
                bytesMessage.putProperties(fieName, valueS);
            } else {
                System.out.println("未定义的类型");
            }
        }
        return bytesMessage;
    }

    public void close() throws IOException {
        memoryMappedFile.close();
    }


}
