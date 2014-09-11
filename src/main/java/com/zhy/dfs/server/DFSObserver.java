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
        DFSServer.getInstance().clearAlivedChannels();
        LinkedHashMap<String, SocketChannel> sharedChannels = DFSServer.getInstance().getSharedChannels();
        ConcurrentHashMap<String, List<SocketChannel>> replications = DFSServer.getInstance().getReplicationChannels();
        List<String> sharedKeys = new ArrayList(sharedChannels.keySet());
        for (String sharedKey : sharedKeys) {
            SocketChannel sharedChannel = sharedChannels.get(sharedKey);
            if (sharedChannel != null) {
                try {
                    DFSServer.getInstance().write(Code.SERVER_HEARTBEAT, sharedChannel);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                List<SocketChannel> sharedRepliChannels = replications.get(sharedKey);
                if (sharedRepliChannels != null && !sharedRepliChannels.isEmpty()) {
                    for (SocketChannel sharedRepliChannel : sharedRepliChannels) {
                        try {
                            DFSServer.getInstance().write(Code.SERVER_HEARTBEAT, sharedRepliChannel);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
//        try {
//            Thread.sleep(1000 * 10);
//            DFSServer.getInstance().alivedToUsed();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
    }
    
    public void aaa() {
        run();
    }

}
