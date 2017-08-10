package com.xhc.test.testzookeeper.baseoperator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;

public class BaseTest {

    private final static String CONNECTSTRING="192.168.127.129:2181,192.168.127.130:2181," +
            "192.168.127.131:2181";
    
    /**
     * 测试节点的增删改查
     * 
     * watcher的触发
     *                          event For "/path"               event For "/path/child"

        create("/path")         EventType.NodeCreated           NA
        delete("/path")         EventType.NodeDeleted           NA
        setData("/path")        EventType.NodeDataChanged       NA
        create("/path/child")   EventType.NodeChildrenChanged   EventType.NodeCreated
        delete("/path/child")   EventType.NodeChildrenChanged   EventType.NodeDeleted
        setData("/path/child")  NA                              EventType.NodeDataChanged
     * 
     * @throws Exception
     */
    public static void createSession() throws Exception {
        CountDownLatch cdl = new CountDownLatch(1);
        Stat stat = new Stat();
        String path = "/temp";
        String path2 = "/temp/a1";
        ZooKeeper zk = new ZooKeeper(CONNECTSTRING, 5000, new Watcher(){
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("watcher event ："+ watchedEvent.getPath() + " , " + watchedEvent.getType() + " , " + watchedEvent.getState());
                if(watchedEvent.getState()==Event.KeeperState.SyncConnected){
                    cdl.countDown();
                }
            } 
        });
        cdl.await();
        
        stat = zk.exists(path, true);
        if(stat == null){
            String p = zk.create(path, "123".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println("创建了：" + p);
        }
        stat = zk.exists(path2, true);
        if(stat == null){
            String p = zk.create(path2, "456".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println("创建了：" + p);
        }
        
        
        byte[] bytes = zk.getData(path, true, stat);
        System.out.println(path + " : " + new String(bytes, "utf-8"));
        
        String newData = "6666";
        System.out.println("set data ["+newData+"] to path :" + path);
        zk.setData(path, newData.getBytes(), -1);
        
        bytes = zk.getData(path, true, stat);
        System.out.println(path + " : " + new String(bytes, "utf-8"));
        
        List<String> list = zk.getChildren(path, true);
        for(String s : list){
            String cpath = path+"/"+s;
            System.out.println(cpath + " : " + new String(zk.getData(cpath, true, stat), "utf-8"));
            zk.delete(cpath, -1);
        }
        zk.delete(path, -1);
    }
    
    
    /**
     *  zookeeper access controller
     *  
     *  一个ACL对象由schema:ID和Permissions组成
     *  
     *  Schema:Id
     *  world: 它下面只有一个id, 叫anyone, world:anyone代表任何人，zookeeper中对所有人有权限的结点就是属于world:anyone的
        auth: 它不需要id, 只要是通过authentication的user都有权限（zookeeper支持通过kerberos来进行authencation, 也支持username/password形式的authentication)
        digest: 它对应的id为username:BASE64(SHA1(password))，它需要先通过username:password形式的authentication
        ip: 它对应的id为客户机的IP地址，设置的时候可以设置一个ip段，比如ip:192.168.1.0/16, 表示匹配前16个bit的IP段
        super: 在这种scheme情况下，对应的id拥有超级权限，可以做任何事情(cdrwa)
     
     *  Perms权限
     *  CREATE: 能创建子节点
        READ：能获取节点数据和列出其子节点
        WRITE: 能设置节点数据
        DELETE: 能删除子节点
        ADMIN: 能设置权限
        
        
     * @throws Exception
     */
    public static void testACL() throws Exception {
        CountDownLatch cdl = new CountDownLatch(1);
        
        ZooKeeper zk = new ZooKeeper(CONNECTSTRING, 5000, new Watcher(){
            @Override
            public void process(WatchedEvent event) {
                System.out.println("watcher event ："+ event.getPath() + " , " + event.getType() + " , " + event.getState());
                if(event.getState()==Event.KeeperState.SyncConnected){
                    cdl.countDown();
                }
            }
        });
        cdl.await();

        Stat stat = new Stat();
        List<ACL> acls=new ArrayList<>();
        Id id = new Id("world","anyone");
        Id id2 = new Id("auth", "" );
        Id id3 = new Id("digest", "root:root");
        Id id4 = new Id("ip" , "192.2168.127.1");
        
        ACL acl=new ACL(ZooDefs.Perms.CREATE, new Id("digest","root:root"));
        ACL acl2=new ACL(ZooDefs.Perms.ALL, new Id("ip","192.168.127.1"));
        acls.add(acl);
        acls.add(acl2);
        
        String path = "/xhc1";
        stat = zk.exists(path, true);
        if(stat == null){
            System.out.println("创建节点：" + path);
            zk.create(path, "123".getBytes(), acls, CreateMode.PERSISTENT);    
        }
        
        System.out.println("设置连接schema:id");
        zk.addAuthInfo("digest","root:root".getBytes());

        stat = zk.exists(path, true);
        System.out.println("设置节点["+path+"]权限：" + acls);
        zk.setACL(path, acls, stat.getVersion());
        
        System.out.println("获取节点["+path+"]的ACL:");
        List<ACL> list = zk.getACL(path, stat);
        for(ACL a : list){
            System.out.println("perm: " + a.getPerms() + ", id: " +a.getId().toString());    
        }
        
        
        byte[] bytes = zk.getData(path, true, stat);
        System.out.println("获取节点[" + path + "] 的值：" + new String(bytes, "utf-8"));
        
        String newData = "hello world";
        System.out.println("修改节点["+path+"]的值为：" + newData.getBytes());
        
        System.out.println("删除节点：" + path);
        zk.delete(path, -1);
        
    }
    



    public static void main(String[] args) throws Exception{
//        createSession();
        testACL();
    }
    
    
}
