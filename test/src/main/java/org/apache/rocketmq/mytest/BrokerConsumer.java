package org.apache.rocketmq.mytest;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;

import java.util.concurrent.TimeUnit;

/**
 * @author: codefans
 * @date: 2018-10-25 09:00
 * 直连broker,并消费消息
 */
public class BrokerConsumer {

    private String consumerGroup = "brokerProducerGroupName";
    private String namesrvAddr = "192.168.199.159:9876";
    private String topic = "mycreateTopic";
    private String tags = "brokerProducerTagsName";

    private int batchMaxSize = 0;

    private DefaultMQPushConsumer defaultConsumer;

    /**
     * 消息监听器列表。
     */
    private MessageListener messageListener;

    public static void main(String[] args) {
        BrokerConsumer brokerConsumer = new BrokerConsumer();
        brokerConsumer.consume();
    }

    public void consume() {

        try {
            defaultConsumer = new DefaultMQPushConsumer(consumerGroup);
            defaultConsumer.setNamesrvAddr(namesrvAddr);
            defaultConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            // 消费者订阅主题。
            defaultConsumer.subscribe(topic, tags);

            // 消息批量条数
            if(batchMaxSize != 0){
                defaultConsumer.setConsumeMessageBatchMaxSize(batchMaxSize);
            }

            // 消费者注册消息监听器。
            if (messageListener == null) {
                messageListener = new BrokerMessageListenerOrderly();
            }

            if (messageListener instanceof MessageListenerConcurrently) {// 无序消息
                defaultConsumer.registerMessageListener((MessageListenerConcurrently) messageListener);
            } else if (messageListener instanceof MessageListenerOrderly) {// 有序消息
                defaultConsumer.registerMessageListener((MessageListenerOrderly) messageListener);
            } else {
                throw new IllegalStateException(
                        "unknown type of " + MessageListener.class + " : [" + messageListener.getClass() + "]");
            }


            Thread startTask = new Thread(new Runnable() {
                @Override
                public void run() {
                    // 启动consumer。
                    try {
                        try {
                            TimeUnit.MILLISECONDS.sleep(3*1000);
                        } catch (InterruptedException ignore) {
                        }
                        defaultConsumer.start();
                        System.out.println("针对Topic[" + topic + "]的RocketMQ消费者启动成功!");
                    } catch (MQClientException e) {
                        e.printStackTrace();
                        System.out.println("针对Topic[" + topic + "]的RocketMQ消费者启动失败!");
                    }
                }
            });
            startTask.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }

    }


}
