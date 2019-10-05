package kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @description: 一定要写注释啊
 * @date: 2019-10-05 18:06
 * @author: 十一
 */
@RestController
public class Producer {
    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    @Value("${kafka.topic}")
    private String topic;

    @GetMapping("/msg")
    public String sendMsg(@RequestParam("msg") String msg) {
        kafkaTemplate.send(topic,msg);
        return "send success";
    }
}
