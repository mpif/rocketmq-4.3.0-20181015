package org.apache.rocketmq.client;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * @Author: ShengzhiCai
 * @Date: 2018-11-03 21:53
 * L1+L2+Header+Body
 * L1: 4个字节, 代表剩余部分的总字节数
 * L2: 4个字节, 低三个字节代表header部分, 最高一个字节代表header的序列化方式:0-JSON;1-自定义序列化方式.
 * Header:
 * Body: 二进制数据块, 长度可通过:L1-L2-4来计算
 */

public class Message {

    private int L1;

    private String data;

    public Message(String data) {
        this.data = data;
    }

    public void write(OutputStream os) {

//        new Integer();


    }

    public void read() {

    }

    public void writeL1(OutputStream os) {

    }

    public void writeL2(OutputStream os) {

    }

    public void writeHeader(OutputStream os) {

    }

    public void writeBody(OutputStream os) {

    }

    public void readL1(InputStream is) {

    }

    public void readL2(InputStream is) {

    }

    public void readHeader(InputStream is) {

    }

    public void readBody(InputStream is) {

    }




}
