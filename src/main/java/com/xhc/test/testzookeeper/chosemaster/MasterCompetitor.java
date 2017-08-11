package com.xhc.test.testzookeeper.chosemaster;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.apache.zookeeper.CreateMode;

public class MasterCompetitor {

    private ZkClient zkClient;
    private String name;
    public static String MASTER_PATH = "/Master";
    
    
    public MasterCompetitor() {
        super();
        // TODO Auto-generated constructor stub
    }

    public MasterCompetitor(ZkClient zkClient, String name) {
        super();
        this.zkClient = zkClient;
        this.name = name;
    }

    public void addWatcher(){
        zkClient.subscribeDataChanges(MASTER_PATH, new IZkDataListener() {
            
            @Override
            public void handleDataChange(String s, Object o) throws Exception {

            }

            @Override
            public void handleDataDeleted(String s) throws Exception {
                System.out.println("master被删除了，["+name+"] 开始争抢master");
                competeMaster();
            }
        });
    }
    
    public boolean competeMaster() {
        try {
            zkClient.create(MASTER_PATH, name, CreateMode.EPHEMERAL_SEQUENTIAL);
        } catch (Exception e) {
            e.printStackTrace();
            if(zkClient.exists(MASTER_PATH)){
                Object data = zkClient.readData(MASTER_PATH, true);
                if(data == null){
                    competeMaster();
                }
            }
        }
        
        return true;
    }
}
