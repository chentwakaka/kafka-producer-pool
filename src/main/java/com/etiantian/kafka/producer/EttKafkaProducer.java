package com.etiantian.kafka.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.apache.commons.pool2.impl.GenericObjectPool;


/**
 * Created by chentong on 2016/9/30.
 */
public class EttKafkaProducer {

    private final GenericObjectPool<Producer<String, String>> pool;

    public EttKafkaProducer(GenericObjectPool<Producer<String, String>> pool) {
        this.pool = pool;
    }

    public void send(KeyedMessage<String, String> message) throws Exception{
        Producer<String, String> producer = null;
        try {
            producer = pool.borrowObject();
            producer.send(message);
        }  finally {
            if (producer != null) {
                pool.returnObject(producer);
            }
        }
    }

}
