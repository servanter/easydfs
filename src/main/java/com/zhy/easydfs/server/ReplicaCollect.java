package com.zhy.easydfs.server;

import java.util.TimerTask;

public class ReplicaCollect extends TimerTask {

    @Override
    public void run() {
        try {
            System.out.println("Start replica collect version");
            EasyDFSServer.getInstance().collectReplicaVersion();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        

    }

}
