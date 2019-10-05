package kafka;

import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * @description: 消费者同步手动提交
 * @date: 2019-10-05 14:57
 * @author: 十一
 */
public class AsyncConsumer extends ShutdownableThread {

    private KafkaConsumer<Integer,String> consumer;

    public AsyncConsumer() {
        // 消费过程中是否会被中断false
        super("KafkaConsumerTest",false);
        // 这些参数都有默认值的
        Properties properties = new Properties();
        // 添加kafka，多个节点用逗号“,”分隔
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        // 消费组id
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"cityGroup-1");
        // 开启自动提交，默认为true，一般不自动提交，自动提交可能会导致消息重复消费
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        // 一次poll的最大值
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"500");
        // 两次心跳的间隔时间，不要超过SESSION_TIMEOUT_MS_CONFIG时间的三分之一
        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,"10000");
        // 会话超时时间
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,"30000");
        // 当kafka中灭有添加offset初始值时，从这里读取offset的值
        // earliest: offset的第一条
        // latest: offset的最后一条
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        // 生产者是序列化，消费者这里需要反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG
                ,"org.apache.kafka.common.serialization.IntegerDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
                ,"org.apache.kafka.common.serialization.StringDeserializer");
        this.consumer = new KafkaConsumer<Integer, String>(properties);
    }

    /**
     * 接收消息
     */
    @Override
    public void doWork() {
        String topic = "cities";
        String topicValue = "suzhou";
        // 订阅消费主题
        consumer.subscribe(Collections.singletonList(topic));
        // 从broker中获取信息，如果没有消息等待1000ms
        ConsumerRecords<Integer, String> records = consumer.poll(1000);
        for (ConsumerRecord<Integer, String> record : records) {
            System.out.println("topic: " + record.topic());
            System.out.println("partition: " + record.partition());
            System.out.println("key: " + record.key());
            System.out.println("value: " + record.value());
            System.out.println("-------=================------------");
        }
        // 采用同步和异步提交的方式可以防止重复消费的问题
        // 异步提交
        consumer.commitAsync((offset,e) -> {
            if (e != null) {
                System.out.println("提交失败");
                e.printStackTrace();

                // 同步提交
                consumer.commitSync();
            }
        });
    }

    public static void main(String[] args) {
        AsyncConsumer consumer = new AsyncConsumer();
        consumer.start();
    }
}
