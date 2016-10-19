package com.etiantian.kafka.util;

import com.etiantian.kafka.pool.KafkaPoolFactory;
import com.etiantian.kafka.producer.EttKafkaProducer;
import kafka.javaapi.producer.Producer;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.util.Properties;

/**
 * Created by chentong on 2016/9/30.
 */
public class KafkaProducerGenerator {

    private final Properties properties;
    private EttKafkaProducer ettKafkaProducer;
    private GenericObjectPool<Producer<String, String>> pool;

    public KafkaProducerGenerator(Properties properties){
        this.properties = properties;
        this.pool = generatePool(properties);
        this.ettKafkaProducer = new EttKafkaProducer(pool);
    }

    protected GenericObjectPool<Producer<String, String>> generatePool(Properties properties) {
        GenericObjectPool<Producer<String, String>> pool = new GenericObjectPool(new KafkaPoolFactory(properties));
        pool.setMinIdle(properties.get("poolMinIdle")==null ? 256 : Integer.parseInt(properties.get("poolMinIdle").toString()));
        pool.setMaxIdle(properties.get("poolMaxIdle")==null ? 512 : Integer.parseInt(properties.get("poolMaxIdle").toString()));
        pool.setMaxTotal(properties.get("poolMaxTotal")==null ? 1024 : Integer.parseInt(properties.get("poolMaxTotal").toString()));
        return pool;
    }

    public EttKafkaProducer getEttKafkaProducer(){
        return this.ettKafkaProducer;
    }

    public void stop(){
        this.pool.close();
    }

    protected GenericObjectPool<Producer<String, String>> getPool(){
        return pool;
    }

    protected void setPool(GenericObjectPool<Producer<String, String>> pool){
        this.pool = pool;
        this.ettKafkaProducer = new EttKafkaProducer(pool);
    }
}
