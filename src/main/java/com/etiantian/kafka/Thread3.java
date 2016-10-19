package com.etiantian.kafka;

/**
 * Created by chentong on 2016/10/11.
 */
public class Thread3 implements Runnable {

    public void run() {

        for(int i=0;i<10;i++){
            KafkaProducerPoolManager.getInstance().send("logTopic","logTopicKey","for loop message +"+(i+1));
        }
    }
}
