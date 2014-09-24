package com.zhy.easydfs.server;

import java.util.TimerTask;

public class RepliSync extends TimerTask {

    @Override
    public void run() {
        System.out.println("Start replica sync.");
        try {
            EasyDFSServer.getInstance().syncReplica();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
