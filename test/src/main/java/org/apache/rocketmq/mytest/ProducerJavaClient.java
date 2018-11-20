package org.apache.rocketmq.mytest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 * @author: ShengzhiCai
 * @date: 2018-11-05 10:12
 */
public class ProducerJavaClient {

    public static void main(String[] args) {
        ProducerJavaClient producerJavaClient = new ProducerJavaClient();
        producerJavaClient.produce();
    }

    public void produce() {

        InputStream is = null;
        OutputStream os = null;

        try {

            String host = "localhost";
            int port = 9876;
            Socket socket = new Socket(host, port);
            socket.setKeepAlive(true);
            socket.setSoTimeout(5 * 1000);
            os = socket.getOutputStream();

            Message mqMsg = new Message("JavaClientMsg");
            mqMsg.write(os);

            byte[] responseMsg = new byte[1024];
            int len = 0;
            ByteArrayOutputStream bao = new ByteArrayOutputStream();

            is = socket.getInputStream();
            while((len = is.read(responseMsg)) != -1) {
                bao.write(responseMsg, 0, len);
            }

            System.out.println(bao.toString("UTF-8"));


        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if(is != null) {
                    is.close();
                    is = null;
                }
                if(os != null) {
                    os.flush();
                    os.close();
                    os = null;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }






}
