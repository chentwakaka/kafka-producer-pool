package com.etiantian.kafka.pool;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.util.Properties;

/**
 * Created by chentong on 2016/9/30.
 */
public class KafkaPoolFactory extends BasePooledObjectFactory<Producer<String,String>> {

    private final Properties properties;

    public KafkaPoolFactory(Properties properties){
        this.properties = properties;
    }

    public Producer<String, String> create() throws Exception {
        return new Producer(new ProducerConfig(properties));
    }

    public PooledObject<Producer<String, String>> wrap(Producer<String, String> stringStringProducer) {
        return new DefaultPooledObject(stringStringProducer);
    }
}
