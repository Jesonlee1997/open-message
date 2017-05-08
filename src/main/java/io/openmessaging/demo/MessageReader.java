package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

import static io.openmessaging.demo.serialize.Constants.*;

/**
 * Created by JesonLee
 * on 2017/5/5.
 */
public class MessageReader {
    private RandomAccessFile memoryMappedFile;
    private static final int MAPPED_SIZE = 64 * 1024;
    private static final int CORDON = 1024;//buffer的警戒线，预防Buffer满上
    private MappedByteBuffer buffer;//初始的buffer
    private final Map<Thread, Input> readers = new HashMap<>();
    private Work work;
    //private ConcurrentHashMap<Integer, MappedByteBuffer> bufferedMap; TODO:Buffer置换

    public MessageReader(String fileName) {
        try {
            memoryMappedFile = new RandomAccessFile(fileName, "rw");
            work = new Work(memoryMappedFile);
            long length = memoryMappedFile.length();
            if (length == 0) {
                System.out.println(fileName + "不存在");//TODO：抛异常
            }
            if (memoryMappedFile.length() > MAPPED_SIZE) {
                buffer = memoryMappedFile.getChannel().map(
                        FileChannel.MapMode.READ_ONLY,
                        0,
                        MAPPED_SIZE);
            } else {
                buffer  = memoryMappedFile.getChannel().map(
                        FileChannel.MapMode.READ_ONLY,
                        0,
                        memoryMappedFile.length());
            }

        } catch (IOException e) {
            System.out.println("MessageReader发生IO异常");
            e.printStackTrace();
        }
    }

    public Message readMessage() {
        Thread thread = Thread.currentThread();
        Input input = readers.get(thread);
        if (input == null) {
            synchronized (readers) {
                input = new Input(buffer, work);
                if (readers.get(thread) == null) {
                    readers.put(thread, input);
                }
            }
        }
        return input.readMessage();
    }

    public void close() throws IOException {
        memoryMappedFile.close();
    }

    class Work {
        private final RandomAccessFile accessFile;

        public Work(RandomAccessFile accessFile) {
            this.accessFile = accessFile;
        }

        public MappedByteBuffer remap(long start) {
            try {
                return  memoryMappedFile.getChannel().map(
                        FileChannel.MapMode.READ_ONLY,
                        start,
                        MAPPED_SIZE);
            } catch (IOException e) {
                System.out.println("remap失败");
                e.printStackTrace();
                System.exit(1);
            }
            return null;
        }
    }

    /**
     * Created by JesonLee
     * on 2017/5/7.
     */
    static class Input {

        private long startPosition;
        private int position = 0;
        private MappedByteBuffer mappedByteBuffer;
        private Work work;

        public Input(MappedByteBuffer mappedByteBuffer, Work work) {
            this.mappedByteBuffer = mappedByteBuffer;//初始的映射值
            this.work = work;
        }

        public Message readMessage() {
            if (MAPPED_SIZE - position <= CORDON) {
                startPosition = startPosition + position;
                position = 0;
                mappedByteBuffer = work.remap(startPosition);
            }
            if (mappedByteBuffer.get(position) != MESSAGESTART) {
                return null;
            }

            //开始反序列化
            BytesMessage bytesMessage = new DefaultBytesMessage();
            byte messageNum = mappedByteBuffer.get(position++);//messageNum应为100
            int bodyLength = mappedByteBuffer.getInt(position);
            position += 4;

            //序列化body
            byte[] bytes = new byte[bodyLength];
            for (int i = 0; i < bodyLength; i++) {
                bytes[i] = mappedByteBuffer.get(position++);
            }
            bytesMessage.setBody(bytes);
            //序列化Headers
            byte headerNum;
            while ((headerNum = mappedByteBuffer.get(position++)) != PROPERTIS_START) {
                //TODO:添加更多的判断，出现频率最高的if-else放在最前面
                if (headerNum == TOPIC) {

                    int length = mappedByteBuffer.getInt(position);
                    position += 4;

                    bytes = new byte[length];
                    for (int i = 0; i < length; i++) {
                        bytes[i] = mappedByteBuffer.get(position++);
                    }
                    String s = new String(bytes);
                    bytesMessage.putHeaders(MessageHeader.TOPIC, s);
                } else if (headerNum == QUEUE) {

                    int length = mappedByteBuffer.getInt(position);
                    position += 4;

                    bytes = new byte[length];
                    for (int i = 0; i < length; i++) {
                        bytes[i] = mappedByteBuffer.get(position++);
                    }
                    String s = new String(bytes);

                    bytesMessage.putHeaders(MessageHeader.QUEUE, s);
                } else if (headerNum == MESSAGE_ID) {

                    long messageId = mappedByteBuffer.getLong(position);
                    position += 8;
                    bytesMessage.putHeaders(MessageHeader.MESSAGE_ID, messageId);
                } else {
                    System.out.println("未定义的消息头");//TODO:抛异常
                }
            }

            //序列化Properties
            while (mappedByteBuffer.get(position) == IS_PROPERTY_FIELD) {
                int length = mappedByteBuffer.getInt(++position);
                position += 4;
                bytes = new byte[length];
                for (int i = 0; i < length; i++) {
                    bytes[i] = mappedByteBuffer.get(position++);
                }
                String fieName = new String(bytes);
                byte type = mappedByteBuffer.get(position++);
                if (type == INT) {
                    int value = mappedByteBuffer.getInt(position);
                    position += 4;
                    bytesMessage.putProperties(fieName, value);
                } else if (type == LONG) {
                    long valueL = mappedByteBuffer.getLong(position);
                    position += 8;
                    bytesMessage.putProperties(fieName, valueL);
                } else if (type == DOUBLE) {
                    double value = mappedByteBuffer.getDouble(position);
                    position += 8;
                    bytesMessage.putProperties(fieName, value);
                } else if (type == STRING) {
                    int valueLength = mappedByteBuffer.getInt(position);
                    position += 4;

                    bytes = new byte[valueLength];
                    for (int i = 0; i < valueLength; i++) {
                        bytes[i] = mappedByteBuffer.get(position++);
                    }
                    String valueS = new String(bytes);
                    bytesMessage.putProperties(fieName, valueS);
                } else {
                    System.out.println("未定义的类型");
                }
            }
            return bytesMessage;
        }
    }
}

