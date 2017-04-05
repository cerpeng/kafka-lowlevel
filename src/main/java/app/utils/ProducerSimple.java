package app.utils;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import java.util.Properties;

import static app.consts.KafkaConsts.*;

/**
 * @author cerpengxi
 * @date 17/3/28 下午2:17
 */

public class ProducerSimple extends Thread {
    private static PropertiesParser prop = new PropertiesParser(CONF_FILE);
    private static String topicJob = prop.getStringProperty(TOPIC_JOB);
    private static String zookeeperConnect = prop.getStringProperty(ZOOKEEPER_CONNECT);
    private static String brokerList = prop.getStringProperty(BROKER_LIST);
    private static Producer<Integer, String> producer;

    private static void init() {
        Properties props = new Properties();
        props.setProperty("zookeeper.connect", zookeeperConnect);
        props.setProperty("serializer.class", StringEncoder.class.getName());
        props.setProperty("metadata.broker.list", brokerList);
        producer = new Producer<>(new ProducerConfig(props));
    }

    public ProducerSimple() {
        super();
    }

    @Override
    public void run() {
        Producer<Integer, String> producer = createProducer();
        for (int i = 1; i < 10; i++) {
            String message = "message" + i;
            producer.send(new KeyedMessage<>(topicJob, message));
            System.out.println("发送：" + message);
            try {
                sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static Producer<Integer, String> createProducer() {
        if (producer == null) {
            init();
        }
        return producer;
    }

    public static void main(String[] args) {
        new ProducerSimple().start();
    }
}