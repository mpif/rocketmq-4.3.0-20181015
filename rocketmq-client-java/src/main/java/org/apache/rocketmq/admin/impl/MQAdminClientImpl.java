package org.apache.rocketmq.admin.impl;

import org.apache.rocketmq.admin.MQAdminClient;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.KVTable;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * @Author: ShengzhiCai
 * @Date: 2018-11-14 5:36
 */

public class MQAdminClientImpl implements MQAdminClient {

    private String namesrvAddr;

    public MQAdminClientImpl(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, namesrvAddr);
    }

    @Override
    public void clusterList() {

        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();

            ClusterInfo clusterInfoSerializeWrapper = defaultMQAdminExt.examineBrokerClusterInfo();

            System.out.printf("%-16s %-22s %-4s %-22s %-16s %19s %19s %10s %5s %6s %14s %14s %14s %14s %n",
                    "#Cluster Name",
                    "#Broker Name",
                    "#BID",
                    "#Addr",
                    "#Version",
                    "#InTPS(LOAD)",
                    "#OutTPS(LOAD)",
                    "#PCWait(ms)",
                    "#Hour",
                    "#SPACE",
                    "#InTotalYest",
                    "#OutTotalYest",
                    "#InTotalToday",
                    "#OutTotalToday"
            );

            Iterator<Map.Entry<String, Set<String>>> itCluster = clusterInfoSerializeWrapper.getClusterAddrTable().entrySet().iterator();
            while (itCluster.hasNext()) {
                Map.Entry<String, Set<String>> next = itCluster.next();
                String clusterName = next.getKey();
                TreeSet<String> brokerNameSet = new TreeSet<String>();
                brokerNameSet.addAll(next.getValue());

                for (String brokerName : brokerNameSet) {
                    BrokerData brokerData = clusterInfoSerializeWrapper.getBrokerAddrTable().get(brokerName);
                    if (brokerData != null) {

                        Iterator<Map.Entry<Long, String>> itAddr = brokerData.getBrokerAddrs().entrySet().iterator();
                        while (itAddr.hasNext()) {
                            Map.Entry<Long, String> next1 = itAddr.next();
                            double in = 0;
                            double out = 0;
                            String version = "";
                            String sendThreadPoolQueueSize = "";
                            String pullThreadPoolQueueSize = "";
                            String sendThreadPoolQueueHeadWaitTimeMills = "";
                            String pullThreadPoolQueueHeadWaitTimeMills = "";
                            String pageCacheLockTimeMills = "";
                            String earliestMessageTimeStamp = "";
                            String commitLogDiskRatio = "";

                            long inTotalYest = 0;
                            long outTotalYest = 0;
                            long inTotalToday = 0;
                            long outTotalToday = 0;

                            try {
                                KVTable kvTable = defaultMQAdminExt.fetchBrokerRuntimeStats(next1.getValue());
                                String putTps = kvTable.getTable().get("putTps");
                                String getTransferedTps = kvTable.getTable().get("getTransferedTps");
                                sendThreadPoolQueueSize = kvTable.getTable().get("sendThreadPoolQueueSize");
                                pullThreadPoolQueueSize = kvTable.getTable().get("pullThreadPoolQueueSize");

                                sendThreadPoolQueueSize = kvTable.getTable().get("sendThreadPoolQueueSize");
                                pullThreadPoolQueueSize = kvTable.getTable().get("pullThreadPoolQueueSize");

                                sendThreadPoolQueueHeadWaitTimeMills = kvTable.getTable().get("sendThreadPoolQueueHeadWaitTimeMills");
                                pullThreadPoolQueueHeadWaitTimeMills = kvTable.getTable().get("pullThreadPoolQueueHeadWaitTimeMills");
                                pageCacheLockTimeMills = kvTable.getTable().get("pageCacheLockTimeMills");
                                earliestMessageTimeStamp = kvTable.getTable().get("earliestMessageTimeStamp");
                                commitLogDiskRatio = kvTable.getTable().get("commitLogDiskRatio");

                                version = kvTable.getTable().get("brokerVersionDesc");
                                {
                                    String[] tpss = putTps.split(" ");
                                    if (tpss.length > 0) {
                                        in = Double.parseDouble(tpss[0]);
                                    }
                                }

                                {
                                    String[] tpss = getTransferedTps.split(" ");
                                    if (tpss.length > 0) {
                                        out = Double.parseDouble(tpss[0]);
                                    }
                                }

                                String msgPutTotalYesterdayMorning = kvTable.getTable().get("msgPutTotalYesterdayMorning");
                                String msgPutTotalTodayMorning = kvTable.getTable().get("msgPutTotalTodayMorning");
                                String msgPutTotalTodayNow = kvTable.getTable().get("msgPutTotalTodayNow");
                                String msgGetTotalYesterdayMorning = kvTable.getTable().get("msgGetTotalYesterdayMorning");
                                String msgGetTotalTodayMorning = kvTable.getTable().get("msgGetTotalTodayMorning");
                                String msgGetTotalTodayNow = kvTable.getTable().get("msgGetTotalTodayNow");

                                inTotalYest = Long.parseLong(msgPutTotalTodayMorning) - Long.parseLong(msgPutTotalYesterdayMorning);
                                outTotalYest = Long.parseLong(msgGetTotalTodayMorning) - Long.parseLong(msgGetTotalYesterdayMorning);

                                inTotalToday = Long.parseLong(msgPutTotalTodayNow) - Long.parseLong(msgPutTotalTodayMorning);
                                outTotalToday = Long.parseLong(msgGetTotalTodayNow) - Long.parseLong(msgGetTotalTodayMorning);

                            } catch (Exception e) {
                                e.printStackTrace();
                            }

                            double hour = 0.0;
                            double space = 0.0;

                            if (earliestMessageTimeStamp != null && earliestMessageTimeStamp.length() > 0) {
                                long mills = System.currentTimeMillis() - Long.valueOf(earliestMessageTimeStamp);
                                hour = mills / 1000.0 / 60.0 / 60.0;
                            }

                            if (commitLogDiskRatio != null && commitLogDiskRatio.length() > 0) {
                                space = Double.valueOf(commitLogDiskRatio);
                            }

                            System.out.printf("%-16s  %-22s  %-4s  %-22s %-16s %19s %19s %10s %5s %6s %14s %14s %14s %14s%n",
                                    clusterName,
                                    brokerName,
                                    next1.getKey(),
                                    next1.getValue(),
                                    version,
                                    String.format("%9.2f(%s,%sms)", in, sendThreadPoolQueueSize, sendThreadPoolQueueHeadWaitTimeMills),
                                    String.format("%9.2f(%s,%sms)", out, pullThreadPoolQueueSize, pullThreadPoolQueueHeadWaitTimeMills),
                                    pageCacheLockTimeMills,
                                    String.format("%2.2f", hour),
                                    String.format("%.4f", space),
                                    inTotalYest,
                                    outTotalYest,
                                    inTotalToday,
                                    outTotalToday
                            );
                        }
                    }
                }

                if (itCluster.hasNext()) {
                    System.out.printf("");
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }



}
