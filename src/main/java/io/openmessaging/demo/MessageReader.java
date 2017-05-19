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

import static io.openmessaging.demo.CustomConstants.*;

/**
 * Created by JesonLee
 * on 2017/5/5.
 */
public class MessageReader {
    private RandomAccessFile memoryMappedFile;
    private static final long MAPPED_SIZE = 16 * 1024 * 1024;//表示一次映射的字节数
    private MappedByteBuffer buffer;//初始的buffer
    private final Map<Thread, Input> readers = new HashMap<>();
    private Work work;

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
                buffer = memoryMappedFile.getChannel().map(
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
        if (readers.get(thread) == null) {
            synchronized (readers) {
                if (readers.get(thread) == null) {
                    readers.put(thread, new Input(buffer, work));
                }
            }
        }
        return readers.get(thread).readMessage();
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
                return memoryMappedFile.getChannel().map(
                        FileChannel.MapMode.READ_ONLY,
                        start,
                        MAPPED_SIZE);
            } catch (IOException e) {
                //System.out.println("remap失败");
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

        private String getString(int length) {
            byte[] bytes = new byte[length];
            for (int i = 0; i < length; i++) {
                bytes[i] = mappedByteBuffer.get(position++);
            }
            return new String(bytes);
        }

        public Message readMessage() {
            if (MAPPED_SIZE - position <= CustomConstants.MAX_MESSAGE_SIZE) {
                startPosition = startPosition + position;
                position = 0;
                mappedByteBuffer = work.remap(startPosition);
                if (mappedByteBuffer == null) {
                    return null;
                }
            }
            if (mappedByteBuffer.get(position) != MESSAGE_START) {
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

            while ((headerNum = mappedByteBuffer.get(position++)) != PROPERTIES_START) {
                switch (headerNum) {
                    case TOPIC:
                        int length = mappedByteBuffer.getInt(position);
                        position += 4;
                        bytesMessage.putHeaders(MessageHeader.TOPIC, getString(length));
                        break;
                    case QUEUE:
                        length = mappedByteBuffer.getInt(position);
                        position += 4;
                        bytesMessage.putHeaders(MessageHeader.QUEUE, getString(length));
                        break;
                    case MESSAGE_ID:
                        length = mappedByteBuffer.getInt(position);
                        position += 4;
                        bytesMessage.putHeaders(MessageHeader.MESSAGE_ID, getString(length));
                        break;
                    case BORN_TIMESTAMP:
                        long l = mappedByteBuffer.getLong(position);
                        position += 8;
                        bytesMessage.putHeaders(MessageHeader.BORN_TIMESTAMP, l);
                        break;
                    case BORN_HOST:
                        length = mappedByteBuffer.getInt(position);
                        position += 4;
                        bytesMessage.putHeaders(MessageHeader.BORN_HOST, getString(length));
                        break;
                    case STORE_TIMESTAMP:
                        l = mappedByteBuffer.getLong(position);
                        position += 8;
                        bytesMessage.putHeaders(MessageHeader.STORE_TIMESTAMP, l);
                        break;
                    case STORE_HOST:
                        length = mappedByteBuffer.getInt(position);
                        position += 4;
                        bytesMessage.putHeaders(MessageHeader.STORE_HOST, getString(length));
                        break;
                    case START_TIME:
                        l = mappedByteBuffer.getLong(position);
                        position += 8;
                        bytesMessage.putHeaders(MessageHeader.START_TIME, l);
                        break;
                    case STOP_TIME:
                        l = mappedByteBuffer.getLong(position);
                        position += 8;
                        bytesMessage.putHeaders(MessageHeader.STOP_TIME, l);
                        break;
                    case TIMEOUT:
                        int i = mappedByteBuffer.getInt(position);
                        position += 4;
                        bytesMessage.putHeaders(MessageHeader.TIMEOUT, i);
                        break;
                    case PRIORITY:
                        i = mappedByteBuffer.getInt(position);
                        position += 4;
                        bytesMessage.putHeaders(MessageHeader.PRIORITY, i);
                        break;
                    case RELIABILITY:
                        i = mappedByteBuffer.getInt(position);
                        position += 4;
                        bytesMessage.putHeaders(MessageHeader.RELIABILITY, i);
                        break;
                    case SEARCH_KEY:
                        length = mappedByteBuffer.getInt(position);
                        position += 4;
                        bytesMessage.putHeaders(MessageHeader.SEARCH_KEY, getString(length));
                        break;
                    case SCHEDULE_EXPRESSION:
                        length = mappedByteBuffer.getInt(position);
                        position += 4;
                        bytesMessage.putHeaders(MessageHeader.SCHEDULE_EXPRESSION, getString(length));
                        break;
                    case SHARDING_KEY:
                        length = mappedByteBuffer.getInt(position);
                        position += 4;
                        bytesMessage.putHeaders(MessageHeader.SHARDING_KEY, getString(length));
                        break;
                    case SHARDING_PARTITION:
                        length = mappedByteBuffer.getInt(position);
                        position += 4;
                        bytesMessage.putHeaders(MessageHeader.SHARDING_PARTITION, getString(length));
                        break;
                    case TRACE_ID:
                        length = mappedByteBuffer.getInt(position);
                        position += 4;
                        bytesMessage.putHeaders(MessageHeader.TRACE_ID, getString(length));
                        break;
                    default:
                        return null;
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
                    //System.out.println("未定义的类型");
                    return null;//这种情况可能是因为程序异常退出
                }
            }
            return bytesMessage;
        }

    }

}