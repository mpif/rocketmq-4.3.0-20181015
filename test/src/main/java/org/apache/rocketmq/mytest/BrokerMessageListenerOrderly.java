package org.apache.rocketmq.mytest;

import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.Charset;
import java.util.List;

/**
 * @author: codefans
 * @date: 2018-10-25 09:52
 */
public class BrokerMessageListenerOrderly implements MessageListenerOrderly {
    @Override
    public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
        for (MessageExt msg : msgs) {
            String msgStr = new String(msg.getBody(), Charset.forName("UTF-8"));
            System.out.println("BrokerMessageListenerOrderly, " + msgStr);
        }
        return ConsumeOrderlyStatus.SUCCESS;
    }
}
