package com.etiantian.kafka;


/**
 * Created by chentong on 2016/9/30.
 */
public class test {
    public static void main(String ... args) throws Exception {

        Thread1 thread1 = new Thread1();
        Thread2 thread2 = new Thread2();
        Thread3 thread3 = new Thread3();
        thread1.start();
        thread2.run();
        thread3.run();

    }
}
