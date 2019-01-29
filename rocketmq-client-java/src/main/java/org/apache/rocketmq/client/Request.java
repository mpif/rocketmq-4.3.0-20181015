package org.apache.rocketmq.client;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.SerializeType;

import java.util.HashMap;

/**
 * @author: codefans
 * @date: 2018-11-22 14:50
 */
public class Request {

    private int code;
    private LanguageCode language = LanguageCode.JAVA;
    private int version = 0;
    private int opaque = 0;
    private int flag = 0;
    private String remark;
    private HashMap<String, String> extFields;
    private transient CommandCustomHeader customHeader;

//    private SerializeType serializeTypeCurrentRPC = serializeTypeConfigInThisServer;

    private transient byte[] body;

}
