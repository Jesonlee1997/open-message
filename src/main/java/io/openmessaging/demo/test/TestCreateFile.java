package io.openmessaging.demo.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * Created by JesonLee
 * on 2017/4/21.
 */
public class TestCreateFile {
    public static void main(String[] args) throws IOException {
        File base = new File("J:\\Github\\openmessagingdemotester\\src\\main\\java\\io\\openmessaging\\demo\\test.sadesfwe");
        File file = new File(base, "test.sadesfwe.txt");
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
        for (int i = 0; i < 100; i++) {
            objectOutputStream.writeObject(("Jeson" + i).getBytes());
        }
    }
}
