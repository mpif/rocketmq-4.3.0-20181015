package org.apache.rocketmq.mytest;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * @author: codefans
 * @date: 2018-10-25 09:00
 * 直连namesrv,并生产消息
 */
public class NamesrvProducer {

    private final String producerGroup = "brokerProducerGroupName";
    private final String namesrvAddr = "localhost:9876";
    private final String topic = "namesrvProducerTopic";
    private final String tags = "";

    private final int retryTimes = 3;
//    private final String instanceName = "leMacLocalNamesrvProducer";
//    private final String clientIP = "10.75.164.61";
    private final String instanceName = null;
    private final String clientIP = null;

    private final String produceMsgContent = "hello namesrv produce message...";

    private DefaultMQProducer defaultProducer;

    public static void main(String[] args) {
        NamesrvProducer namesrvProducer = new NamesrvProducer();
        namesrvProducer.produce();
    }

    public void produce() {

        try {
            this.init();
            this.sendMsg();
        } catch (MQClientException e) {
            e.printStackTrace();
        }


    }

    public void init() throws MQClientException {

        defaultProducer = new DefaultMQProducer(producerGroup);
        defaultProducer.setNamesrvAddr(namesrvAddr);

        if (retryTimes != 0) {
            // 失败的 重新发送，默认2次
            defaultProducer.setRetryTimesWhenSendFailed(retryTimes);
            defaultProducer.setRetryTimesWhenSendAsyncFailed(retryTimes);
        }

        // 设置实例的名字，便于问题定位
        if (instanceName != null) {
            defaultProducer.setInstanceName(instanceName);
        }

        // 客户端的IP地址
        if(clientIP != null) {
            defaultProducer.setClientIP(clientIP);
        }

        defaultProducer.start();

    }

    public void sendMsg() {

        //send MQ (如果send失败，再 send两次)
        try {
            SendResult sendResult;
            try {

                Message mqMsg =  new Message(topic, tags, produceMsgContent.getBytes("utf-8"));

                sendResult = defaultProducer.send(mqMsg);
                System.out.println(sendResult);

                if(SendStatus.SEND_OK.equals(sendResult.getSendStatus())) {
                    System.out.println("消息发送成功.");
                    defaultProducer.shutdown();
                    System.out.println("生产者关闭.");
                }

                // 如果消息没有发送成功，再发送两次
                if (!SendStatus.SEND_OK.equals(sendResult.getSendStatus())) {
                    sendResult = defaultProducer.send(mqMsg);
                    if (!SendStatus.SEND_OK.equals(sendResult.getSendStatus())) {
                        sendResult = defaultProducer.send(mqMsg);
                    }
                }

            } catch (MQClientException e) {
                e.printStackTrace();
            } catch (RemotingException e) {
                e.printStackTrace();
            } catch (MQBrokerException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
