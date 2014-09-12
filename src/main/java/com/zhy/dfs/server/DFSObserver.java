package com.zhy.dfs.server;

import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import com.zhy.dfs.constants.Code;

/**
 * Monitor client state
 * 
 * @author zhanghongyan
 * 
 */
public class DFSObserver extends TimerTask {

    @Override
    public void run() {
        System.out.println("run");
        try {
            DFSServer.getInstance().boardCast();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            Thread.sleep(1000 * 5);
            System.out.println("sleep over");
            DFSServer.getInstance().alivedToUsed();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    

}
