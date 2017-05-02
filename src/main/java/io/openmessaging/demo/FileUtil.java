package io.openmessaging.demo;

import java.io.*;

/**
 * Created by JesonLee
 * on 2017/4/19.
 */
public class FileUtil {
    //默认file已经存在
    public static Object load(File file) throws IOException, ClassNotFoundException {
        ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(file));
        return objectInputStream.readObject();
    }

    public static ObjectOutputStream createObjectOutputStream(File file) throws IOException {
        if (!file.exists()) {
            file.createNewFile();
        }
        return new ObjectOutputStream(new FileOutputStream(file));
    }
}
