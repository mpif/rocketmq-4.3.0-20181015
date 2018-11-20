package org.apache.rocketmq.admin;

import org.apache.rocketmq.admin.impl.MQAdminClientImpl;
import org.junit.Before;
import org.junit.Test;

/**
 * @Author: ShengzhiCai
 * @Date: 2018-11-14 5:36
 */

public class MQAdminClientTest {

    private String namesrvAddr;
    private MQAdminClient mqClusterAdmin;

    @Before
    public void before() {
        namesrvAddr = "localhost:9876";
        mqClusterAdmin = new MQAdminClientImpl(namesrvAddr);
    }

    @Test
    public void clusterListTest() {

        mqClusterAdmin.clusterList();

    }

    @Test
    public void topicListTest() {
        mqClusterAdmin.topicList();
    }

    @Test
    public void topicClusterListTest() {
        String topic = "namesrvProducerTopic";
        mqClusterAdmin.topicClusterList(topic);
    }

}
