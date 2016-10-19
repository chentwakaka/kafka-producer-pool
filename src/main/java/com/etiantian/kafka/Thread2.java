package com.etiantian.kafka;

/**
 * Created by chentong on 2016/10/11.
 */
public class Thread2 implements Runnable {

    public void run() {
        KafkaProducerPoolManager.getInstance().send("logTopic","logTopicKey","Runnable");
    }
}
