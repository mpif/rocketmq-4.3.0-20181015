package org.apache.rocketmq.mytest;

import com.google.common.annotations.VisibleForTesting;

/**
 * @author: ShengzhiCai
 * @date: 2018-10-25 09:00
 * 直连namesrv,并消费消息
 */
public class NamesrvConsumer {

    public static void main(String[] args) {
        NamesrvConsumer namesrvConsumer = new NamesrvConsumer();
        namesrvConsumer.consume();
    }

    public void consume() {

    }

}
