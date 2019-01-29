package org.apache.rocketmq.admin;

import org.junit.Test;

/**
 * @Author: codefans
 * @Date: 2018-11-14 6:40
 */

public class CommonTest {

    @Test
    public void printfTest() {
//        System.out.printf("%16s% %14s%n", "#Name", "#Password");

        System.out.printf("%-16s %-22s %-4s %-22s %-16s %19s %19s %10s %5s %6s %14s %14s %14s %14s%n",
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
    }
}
