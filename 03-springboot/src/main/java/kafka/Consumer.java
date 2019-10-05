package kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * @description: Kafka消费
 * @date: 2019-10-05 18:10
 * @author: 十一
 */
@Component
public class Consumer {

    @KafkaListener(topics = "${kafka.topic}")
    public void onMsg(String msg) {
        System.out.println("Kafka接收到消息：" + msg);
    }
}
