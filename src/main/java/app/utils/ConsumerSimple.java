package app.utils;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static app.consts.KafkaConsts.*;

/**
 * @author cerpengxi
 * @date 17/3/28 下午2:50
 */

public class ConsumerSimple extends Thread {
    private static PropertiesParser prop = new PropertiesParser(CONF_FILE);
    private static String topicJob = prop.getStringProperty(TOPIC_JOB);
    private static String zookeeperConnect = prop.getStringProperty(ZOOKEEPER_CONNECT);
    private static String groupId = prop.getStringProperty(GROUP_ID);

    private static ConsumerConnector consumerConnect;


    private ConsumerSimple() {
        super();
    }

    private static void init() {
        Properties properties = new Properties();
        properties.setProperty("zookeeper.connect", zookeeperConnect);
        properties.setProperty("group.id", groupId);
        consumerConnect = Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
    }

    @Override
    public void run() {
        ConsumerConnector consumer = createConsumer();
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topicJob, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> MessageStreams = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> kafkaStream = MessageStreams.get(topicJob).get(0);
        ConsumerIterator<byte[], byte[]> iterator = kafkaStream.iterator();
        while (iterator.hasNext()) {
            String message = new String(iterator.next().message());
            System.out.println("接收到：" + message);
        }
    }

    public ConsumerConnector createConsumer() {
        if (consumerConnect == null) {
            init();
        }
        return consumerConnect;
    }

    public static void main(String[] args) {
        new ConsumerSimple().start();
    }
}
