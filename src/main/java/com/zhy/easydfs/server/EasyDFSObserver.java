package com.zhy.easydfs.server;

import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import com.zhy.easydfs.constants.Code;

/**
 * Monitor client state
 * 
 * @author zhanghongyan
 * 
 */
public class EasyDFSObserver extends TimerTask {

    @Override
    public void run() {
        System.out.println("run");
        try {
            EasyDFSServer.getInstance().boardCast();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            Thread.sleep(1000 * 5);
            System.out.println("sleep over");
            EasyDFSServer.getInstance().alivedToUsed();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    

}
