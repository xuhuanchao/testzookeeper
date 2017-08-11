package com.xhc.test.testzookeeper.lock;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ZookeeperUtil {

    
    private final static String CONNECTSTRING = "192.168.127.129:2181,192.168.127.130:2181," +
            "192.168.127.131:2181";
    private static int sessionTimeout = 5000;
    
    public static ZooKeeper getInstance(int sessionTimeout) throws IOException, InterruptedException{
        CountDownLatch cdl = new CountDownLatch(1);
        int time = sessionTimeout;
        time = time == 0 ? ZookeeperUtil.sessionTimeout : time;  
        ZooKeeper zk = new ZooKeeper(CONNECTSTRING, time, new Watcher(){
           @Override
            public void process(WatchedEvent event) {
               if(event.getState()== Event.KeeperState.SyncConnected){
                   cdl.countDown();
               }
            }
        });
        cdl.await();
        return zk;
    }
    
}
