package com.etiantian.kafka;

import com.etiantian.kafka.KafkaProducerPoolManager;

/**
 * Created by chentong on 2016/10/11.
 */
public class Thread1 extends Thread {
    public void run(){
        KafkaProducerPoolManager.getInstance().send("logTopic","logTopicKey","thread");
    }
}
