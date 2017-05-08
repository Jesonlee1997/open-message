package io.openmessaging.demo.test;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by JesonLee
 * on 2017/5/8.
 */
public class TestListRemove {
    @Test
    public void test1() {
        List<String> list = new ArrayList<String>(){{
            for (int i = 0; i < 10; i++) {
                add("queue"+i);
            }
        }};

        String s = list.get(3);
        System.out.println(s);
        list.remove(s);
        System.out.println(list.get(3));

    }
}
