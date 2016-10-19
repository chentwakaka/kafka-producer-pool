package com.etiantian.kafka.producer;


import kafka.producer.Partitioner;

import java.util.Random;


/**
 * Created by chentong on 2016/10/13.
 */
public class EttPartitioner implements Partitioner {

    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(EttPartitioner.class);

    public int partition(Object o, int i) {
        if(o==null){
            Random random = new Random();
            logger.info("key is null ");
            return random.nextInt(i);
        }else{
            int result = Math.abs(o.hashCode())%i;
            logger.info("key is "+ o.toString()+ " partitions is "+ result);
            return result;
        }
    }
}
