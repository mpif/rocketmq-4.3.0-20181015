package org.apache.rocketmq.mytest;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: codefans
 * @date: 2018-11-05 11:25
 */
public class JsonHeader {


    private int code;

    /**
     * 2
     */
    private int flag;

    /**
     * JAVA
     */
    private String language;

    /**
     * 0
     */
    private int opaque;

    /**
     * JSON
     */
    private String erializeTypeCurrentRPC;

    /**
     * 备注
     */
    private String remark;

    /**
     * 0
     */
    private int version;

//    {
//        "code":10,
//        "extFields":{
//            "topic":"Topic_sample",
//            "flag":"4",
//            "bornTimestamp":"1534853535790",
//            "queueId":"1",
//            "batch":"false",
//            "unitMode":"false",
//            "sysFlag":"6",
//            "producerGroup":"ProducerGroup_sample"
//         },
//    }

    private Map<String, String> extFields = new HashMap<String, String>();

    public JsonHeader() {

        code = 10;
        flag = 2;
        language = "JAVA";
        opaque = 0;
        erializeTypeCurrentRPC = "JSON";
        remark = "rocketmq客户端java版,发送消息到rocketmq,测试备注";
        version = 0;

        extFields.put("topic", "namesrvProducerTopic");
        extFields.put("flag", "4");
        extFields.put("bornTimestamp", System.currentTimeMillis()+"");
        extFields.put("queueId", "1");
        extFields.put("batch", "false");
        extFields.put("unitMode", "false");
        extFields.put("sysFlag", "6");
        extFields.put("producerGroup", "namesrvProducerGroupName");

    }


}
