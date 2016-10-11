package com.etiantian.kafka;

/**
 * Created by chentong on 2016/10/11.
 */
public class Thread3 implements Runnable {
    @Override
    public void run() {

        for(int i=0;i<10;i++){
            KafkaProducerPoolManager.getInstance().send("logTopic","for loop message +"+(i+1));
        }
    }
}
