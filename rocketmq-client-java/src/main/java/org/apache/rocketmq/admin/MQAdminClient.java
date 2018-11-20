package org.apache.rocketmq.admin;

/**
 * @Author: ShengzhiCai
 * @Date: 2018-11-14 5:35
 */
public interface MQAdminClient {

    /**
     * List all of clusters
     */
    public void clusterList();

    /**
     * Get cluster info for topic
     */
    public void topicClusterList(String topic);

    /**
     * Fetch broker runtime status data
     */
    public void brokerStatus();

    /**
     * Fetch broker consume stats data
     */
    public void brokerConsumeStats();

    /**
     *
     */
    public void consumerProgress();

    /**
     * Query producer's socket connection and client version
     */
    public void producerConnection();

    /**
     * Query consumer's socket connection, client version and subscription
     */
    public void consumerConnection();

    /**
     * Query consumer's internal data structure
     */
    public void consumerStatus();

    /**
     * Clone offset from other group
     */
    public void cloneGroupOffset();


    /**
     * Topic and Consumer tps stats
     */
    public void statsAll();

    /**
     * Fetch all topic list from name server
     */
    public void topicList();

    /**
     * Examine topic route info
     */
    public void topicRoute();

    /**
     * Examine topic Status info
     */
    public void topicStatus();

    /**
     * Query Message by Id
     */
    public void queryMsgById();

    /**
     * Query Message by Key
     */
    public void queryMsgByKey();

    /**
     * Query Message by Unique key
     */
    public void queryMsgByUniqueKey();

    /**
     * Query Message by offset
     */
    public void queryMsgByOffset();

    /**
     * Print Message Detail
     */
    public void printMsg();

    /**
     * Send msg to broker
     */
    public void sendMsgStatus();

    /**
     * Start Monitoring
     */
    public void startMonitoring();

    /**
     * Synchronize wiki and issue to github.com
     */
    public void syncDocs();

    /**
     * Allocate MQ
     */
    public void allocateMQ();

    /**
     * Check message send response time
     */
    public void checkMsgSendRT();

    /**
     * List All clusters Message Send RT
     */
    public void clusterRT();


}
