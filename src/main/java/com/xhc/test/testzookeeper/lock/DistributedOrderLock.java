package com.xhc.test.testzookeeper.lock;

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;


/**
 * 模拟分布式时序锁的使用
 * 利用zookeeper的临时顺序节点特性，多个线程在同一个节点下建立临时顺序节点，取建立最小节点的线程为获取锁的线程
 * 当线程使用完锁后删除锁创建的临时节点，其他未获取锁的线程监听其所建顺序节点的前一个比自己小的节点，
 * 当该节点删除后一般情况下代表自己建立的顺序节点是最小的了，从而获得锁
 * 
 * @author xhc
 *
 */
public class DistributedOrderLock {

    private ZooKeeper zk;
    private static String lockPath = "/OrderedLock";
    private String keyNode ;
    private int sessionTimeout=5000;
    
    private void init(){
        try {
            sessionTimeout = 5000;
            zk = ZookeeperUtil.getInstance(5000);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    
    private SortedSet getSortedSet(List<String> list) {
        SortedSet<String> sortedSet = new TreeSet<>();
        if(list != null && list.size() > 0){
            for(String s : list){
                sortedSet.add(lockPath + "/" + s);
            }
        }
        return sortedSet;
    }
    
    private void createKeyNode() throws KeeperException, InterruptedException{
        String keyNode = zk.create(lockPath + "/", Thread.currentThread().getName().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);    
        System.out.println(Thread.currentThread().getName() + "创建了有序临时节点: " + keyNode);
        this.keyNode = keyNode;
    }

    
    public DistributedOrderLock(){
        super();
        init();
    }
    
    public boolean lock() throws KeeperException, InterruptedException{
        if(keyNode == null){
            createKeyNode();
        }
        
        List<String> childrens = zk.getChildren(lockPath, true);

        SortedSet<String> sortedSet = getSortedSet(childrens);
        if(sortedSet.size() <= 0){
            return false;
        }
        if(sortedSet.first().equals(keyNode)){
            return true;
        }else {
            SortedSet<String> lessThanKeyNodeSet = sortedSet.headSet(keyNode);
            if(lessThanKeyNodeSet != null){
                CountDownLatch cdl1 = new CountDownLatch(1);
                String preNode = lessThanKeyNodeSet.last();
                zk.exists(preNode ,  new Watcher(){
                    @Override
                    public void process(WatchedEvent event) {
                        if(event.getType() == Event.EventType.NodeDeleted){
                            cdl1.countDown();
                        }
                    }
                });
                cdl1.await();
                return lock();
            }else {
                return false;
            }
        }
    }
    
    
    public void unlock() throws InterruptedException, KeeperException{
        zk.delete(keyNode, -1);
        System.out.println(Thread.currentThread().getName() + "删除了锁 :" + keyNode);
    }
    
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        Stat stat = new Stat();
        ZooKeeper zk = ZookeeperUtil.getInstance(5000);
        stat = zk.exists(lockPath, true);
        if(stat == null){
            zk.create(lockPath, "OrderLock".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        
        int num = 10;
        CountDownLatch cdl = new CountDownLatch(num);
        for(int i=0; i<num; i++){
            new Thread(()->{
                try {
                    DistributedOrderLock distributedLock = new DistributedOrderLock();
                    cdl.countDown();
                    cdl.await();
                    
                    if(distributedLock.lock()){
                        System.out.println(Thread.currentThread().getName() + " 获得了分布式顺序锁，节点：" + distributedLock.getKeyNode());
                        Thread.sleep(5000);
                        distributedLock.unlock();
                    }
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }, "thread-name-"+i).start();
        }
    }

    public static String getLockPath() {
        return lockPath;
    }

    public static void setLockPath(String lockPath) {
        DistributedOrderLock.lockPath = lockPath;
    }

    public String getKeyNode() {
        return keyNode;
    }

    public void setKeyNode(String keyNode) {
        this.keyNode = keyNode;
    }
    
    
}
