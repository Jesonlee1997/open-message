package io.openmessaging.demo;

import io.openmessaging.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by JesonLee
 * on 2017/4/24.
 */
public class BucketMessages {
    private int head = 0;
    private int tail = 0;
    private List<Message> messages = new ArrayList<>();
    private Map<String, Integer> offsetMap;
    private Map<Integer, Integer> offsetNumMap= new TreeMap<>();

    public Message pullMessage(String queue) {
        Integer offset = offsetMap.get(queue);
        if (offset == null) {
            offsetMap.put(queue, 1);
            return messages.get(0);
        }
        Message message = messages.get(offset);
        offsetMap.put(queue, ++offset);
        return message;
    }



}
