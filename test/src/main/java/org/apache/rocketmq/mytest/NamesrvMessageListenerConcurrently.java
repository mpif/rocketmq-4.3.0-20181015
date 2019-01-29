package org.apache.rocketmq.mytest;

import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.Charset;
import java.util.List;

/**
 * @author: codefans
 * @date: 2018-10-25 09:46
 * namesrv消费监听器
 */
public class NamesrvMessageListenerConcurrently implements MessageListenerConcurrently {

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        for (MessageExt msg : msgs) {
            String msgStr = new String(msg.getBody(), Charset.forName("UTF-8"));
            System.out.println("NamesrvMessageListenerConcurrently, " + msgStr);
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }


}
