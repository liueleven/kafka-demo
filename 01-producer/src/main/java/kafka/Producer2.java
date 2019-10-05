package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

/**
 * @description: 生产者
 * @date: 2019-10-05 14:57
 * @author: 十一
 */
public class Producer2 {

    /**
     * 生产的消息类型,泛型代表消息的key和value
     */
    private KafkaProducer<Integer,String> producer;

    public Producer2() {
        Properties properties = new Properties();
        // 添加kafka，多个节点用逗号“,”分隔
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG
                ,"org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG
                ,"org.apache.kafka.common.serialization.StringSerializer");
        // 批量发送，每10条发送一次
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,10);
        // 每50ms发送一次
        properties.put(ProducerConfig.LINGER_MS_CONFIG,50);
        this.producer = new KafkaProducer<Integer, String>(properties);
    }


    /**
     * 发送消息回到
     */
    public void sendMsgCallback() {
        String topic = "cities";
        String topicValue = "suzhou";
        // 主题、partition、key、消息
        ProducerRecord<Integer, String> record =
                new ProducerRecord<Integer, String>(topic,0,1,topicValue);
        // 批量发送消息，消息堆在内存中，根据配置条件来触发kafka进行发送
        for (int i=0; i<50; i++) {
            this.producer.send(record,(data,e) -> {
                if (e == null) {
                    System.out.println("topic: " + data.topic());
                    System.out.println("partition: " + data.partition());
                    System.out.println("offset: " + data.offset());
                }
            });
        }
    }

    public static void main(String[] args) throws IOException {
        Producer2 producer = new Producer2();
//        producer.sendMsg();
        producer.sendMsgCallback();
        System.in.read();
    }
}
