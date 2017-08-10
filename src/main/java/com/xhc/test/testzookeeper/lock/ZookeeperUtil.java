package com.xhc.test.testzookeeper.lock;

import java.io.IOException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ZookeeperUtil {

    
    private final static String CONNECTSTRING = "192.168.127.129:2181,192.168.127.130:2181," +
            "192.168.127.131:2181";
    
    public static ZooKeeper getInstance(Watcher watcher) throws IOException{
        ZooKeeper zk = null;
        if(watcher != null){
            zk = new ZooKeeper(CONNECTSTRING, 5000, watcher);    
        }else {
            zk = new ZooKeeper(CONNECTSTRING, 5000, getDefaultWatcher());
        }
        return zk;
    }
    
    
    public static Watcher getDefaultWatcher() {
        Watcher watcher = new Watcher(){
            @Override
            public void process(WatchedEvent event) {
                System.out.println("watcher event ："+ event.getPath() + " , " + event.getType() + " , " + event.getState());
            }
        };
        return watcher;
    }
}
