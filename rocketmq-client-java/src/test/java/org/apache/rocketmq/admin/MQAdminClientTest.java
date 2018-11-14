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
        namesrvAddr = "192.168.199.159:9876";
        mqClusterAdmin = new MQAdminClientImpl(namesrvAddr);
    }

    @Test
    public void clusterListTest() {

        mqClusterAdmin.clusterList();

    }


}
