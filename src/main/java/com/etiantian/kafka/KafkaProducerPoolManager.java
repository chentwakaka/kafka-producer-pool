package com.etiantian.kafka;

import com.etiantian.kafka.producer.EttKafkaProducer;
import com.etiantian.kafka.util.KafkaProducerGenerator;
import kafka.producer.KeyedMessage;

import java.util.Properties;

/**
 * Created by chentong on 2016/10/10.
 */
public class KafkaProducerPoolManager {

    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(KafkaProducerPoolManager.class);

    private static final KafkaProducerPoolManager instance = new KafkaProducerPoolManager();

    private static KafkaProducerGenerator kafkaProducerGenerator;

    private static Properties kafkaProperties;

    //加载producer配置文件
    static {
        kafkaProperties = new Properties();
        try{

            kafkaProperties.put("metadata.broker.list","192.168.136.128:9092");
            kafkaProperties.put("partitioner.class","com.etiantian.kafka.producer.EttPartitioner");
            kafkaProperties.put("producer.type","sync");
            kafkaProperties.put("request.required.acks","0");
            kafkaProperties.put("message.send.max.retries","3");
            kafkaProperties.put("topic.metadata.refresh.interval.ms","600000");
            kafkaProperties.put("compression.codec","snappy");
            kafkaProperties.put("serializer.class","kafka.serializer.StringEncoder");
            kafkaProperties.put("queue.buffering.max.ms","5000");
            kafkaProperties.put("queue.buffering.max.messages","10000");
            kafkaProperties.put("queue.enqueue.timeout.ms","-1");
            kafkaProperties.put("batch.num.messages","2000");
            kafkaProperties.put("poolMinIdle","256");
            kafkaProperties.put("poolMaxIdle","512");
            kafkaProperties.put("poolMaxTotal","1024");
            //InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("./producer.properties");
            //kafkaProperties.load(is);
        }catch (Exception e){
            logger.error(e.getMessage());
        }
    }


    /**
     * 双重检查加锁，提高了单纯使用synchronized的性能
     * @return
     */
    public static KafkaProducerPoolManager getInstance() {
        return instance;
    }

    /**
     * 初始化 kafkaProducer pool
     */
    public void init() {

        try{
            kafkaProducerGenerator = new KafkaProducerGenerator(kafkaProperties);
            logger.info("init kafkaProducerGenerator success");
        }catch (Exception e){
            logger.error(e);
        }
    }

    /**
     * send kafkamessage
     * @param topicName
     * @param message
     */
    public void send(String topicName,String key,String message) {
        if(topicName == null || message == null){
            return;
        }
        try{
            EttKafkaProducer producer = getKafkaProducer();
            if(producer!=null)
                producer.send(new KeyedMessage(topicName,key,message));

            logger.info("topicName="+topicName+"#####message="+message);
        }catch (Exception e){
            logger.error(message);
            logger.error(e.getMessage());
        }


    }

    /**
     * get producer
     * @return
     */
    public EttKafkaProducer getKafkaProducer(){

        try{
            if(kafkaProducerGenerator==null){
                logger.info("kafkaProducerGenerator is null to init**********");
                init();
            }
            return kafkaProducerGenerator.getEttKafkaProducer();
        }catch (Exception e){
            logger.error(e);
        }
        return null;
    }
}
