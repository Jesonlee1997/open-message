
赛题分析  
————Producer需要实现————  
创建一个消息，给消息指定Topic（可以由多个Consumer消费）  
BytesMessage createBytesMessageToTopic(String topic, byte[] body);
  
创建一个消息，给消息指定Queue（只能由一个Consumer消费）  
BytesMessage createBytesMessageToQueue(String queue, byte[] body);

同步（顺序）的发送消息，message中应当包含目的地（Queue，Topic只能选其一）
void send(Message message);  


————PullConsumer需要实现————  
绑定到一个Queue，并订阅topics，即从这些topic读取消息，绑定到这个Queue后，只能从这个
void attachQueue(String queueName, Collection<String> topics);

规范要求实现阻塞的接口，由properties来设置阻塞时间，但本赛题不需要用到该特性，
请实现一个非阻塞(也即阻塞时间为0)调用, 也即没有消息则返回null
Message poll();


————测试流程————  
创建Topic，创建Queue
创意Producer，Producer创建指定Topic和指定Queue的Message，调用send发送  

将数据保存到磁盘中  

kill Producer进程，另取进程进行消费
创建PullConsumer线程进行消费，一个Consumer对应一个线程，Consumer连接到一个Queue，可以订阅多个Topic。
不断的调用poll拉取队列的消息，直到完全读完，读取的消息要相对有序。


————————  
一个Producer对应一个线程，线程先create对应的Message，再将Message send到对应的队列或topic中  
一个队列对应一个消费者  

————技术难点剖析————  
1. 并发写，并发读
2. 如何序列化反序列化
3. 用什么方式访问磁盘
4. 大量的消息产生要如何处理

针对难点2：  
如何序列化&反序列化  
如何对Message实现序列化  
对BytesMessage中的body进行序列化    
对Headers进行序列化    
对Propertis进行序列化  

二进制协议设计  
![image](https://github.com/Jesonlee1997/open-message/raw/dev2/序列化协议.png)
消息头编号100 用来判断消息的开始  
消息主体长度就是body的size  
消息主体之间与Headers之间没有间隔，body之后立即就是消息的Headers中的字段。  
MessageHeaders有17个字段，用1-17表示这17个字段，已知字段便已知类型，所以没有说明字段的值类型的位。  
表示String类型的值需要两个区域，用一个int(四个字节)表示字段长度，紧接着就是String类型的字节数组。
使用101表示properties属性的开始。
properties用Map的形式来序列化，用一个说明属性位来说明接下来的字段是properties中的字段。
