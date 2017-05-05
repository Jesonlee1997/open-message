package io.openmessaging.demo.test;

import org.junit.Test;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by JesonLee
 * on 2017/5/4.
 */
public class TestMmap {
    private static int count = 100000; // 10 MB

    @Test
    public void test1() throws IOException {

        long start = System.currentTimeMillis();
        RandomAccessFile memoryMappedFile = new RandomAccessFile("largeFile.txt", "rw");
        // Mapping a file into memory
        MappedByteBuffer out = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, count);
        // Writing into Memory Mapped File
        byte[] bytes = new byte[count];
        for (int i = 0; i < count; i++) {
            bytes[i] = 'B';
            //out.put((byte) 'A');
        }
        /*for (int i = 0; i < 10; i++) {
            System.out.print((char) out.get(i));
        }*/
        out.put(bytes);
        memoryMappedFile.close();
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }

    @Test
    public void test2() throws IOException {
        long start = System.currentTimeMillis();
        File file = new File("largeFile2.txt");
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        byte[] bytes = new byte[count];
        for (int i = 0; i < count; i++) {
            bytes[i] = 'A';
        }
        fileOutputStream.write(bytes);
        fileOutputStream.close();
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }
}
