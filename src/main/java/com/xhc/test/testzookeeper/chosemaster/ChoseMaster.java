package com.xhc.test.testzookeeper.chosemaster;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;

public class ChoseMaster {

    public static String CONNECTSTRING = "192.168.127.129:2181,192.168.127.130:2181," +
            "192.168.127.131:2181";
    
    public static void main(String[] args) {
        for(int i=0; i<10; i++){
            ZkClient zkClient = new ZkClient(CONNECTSTRING, 5000, 5000, new SerializableSerializer());
        }
    }
}
