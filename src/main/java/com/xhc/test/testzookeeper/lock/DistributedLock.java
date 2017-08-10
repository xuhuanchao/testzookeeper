package com.xhc.test.testzookeeper.lock;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

public class DistributedLock {

   
    
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        String keyPath = "/DistributedLock";
        Stat stat = new Stat(); 
        int num = 10;
        CountDownLatch cdl = new CountDownLatch(num);
        for(int i=0; i<num; i++){
            new Thread(()->{
                try {
                    ZooKeeper zk = ZookeeperUtil.getInstance(null);
                    cdl.countDown();
                    cdl.await();
                    zk.create(keyPath, Thread.currentThread().getName().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);    
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }, "thread-name-"+i).start();
        }
        
        ZooKeeper zk1 = ZookeeperUtil.getInstance(null);
        byte[] bytes = zk1.getData(keyPath, true, stat);
        System.out.println(keyPath + " : " + new String(bytes, "utf-8"));
    }
}
