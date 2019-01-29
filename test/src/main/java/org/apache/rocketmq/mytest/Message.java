package org.apache.rocketmq.mytest;

import com.alibaba.fastjson.JSON;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author: codefans
 * @date: 2018-11-05 10:51
 */
public class Message {

    private String data;

    public Message(String data) {
        this.data = data;
    }

    public void write(OutputStream os) throws IOException {

        String jsonHeaderStr = JSON.toJSONString(new JsonHeader());
        int headerBytes = jsonHeaderStr.getBytes().length;
        int bodyBytes = data.getBytes().length;

        int L1 = 4 + headerBytes + bodyBytes;
        writeInt(os, L1);
        writeInt(os, headerBytes);
        writeString(os, jsonHeaderStr);
        writeString(os, data);


    }

    public void read(InputStream is) {

    }

    public void writeInt(OutputStream os, int data) throws IOException {
        os.write(data>>0);
        os.write(data>>8);
        os.write(data>>16);
        os.write(data>>24);
    }

    public void writeString(OutputStream os, String data) throws IOException {
        os.write(data.getBytes());
    }


}
