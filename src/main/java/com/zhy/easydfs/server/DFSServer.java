package com.zhy.easydfs.server;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import javax.xml.ws.handler.MessageContext.Scope;

import com.zhy.easydfs.constants.Code;
import com.zhy.easydfs.file.File;
import com.zhy.easydfs.util.NumberUtils;
import com.zhy.easydfs.util.TemplateUtils;

/**
 * server
 * 
 * @author zhanghongyan
 * 
 */
public class DFSServer {

    /**
     * every five minutes
     */
    public static final int TIME = 1000 * 10;

    private static DFSServer server = null;

    private Selector selector;

    /**
     * all connected clients
     */
    private ServerSocketChannel serverChannel;

    /**
     * shared channels
     */
    private LinkedHashMap<String, SocketChannel> sharedChannels = new LinkedHashMap<String, SocketChannel>();

    /**
     * replication channels
     */
    private LinkedHashMap<String, List<SocketChannel>> replicationChannels = new LinkedHashMap<String, List<SocketChannel>>();

    /**
     * current alive shared channels
     */
    private LinkedHashMap<String, SocketChannel> aliveSharedChannels = new LinkedHashMap<String, SocketChannel>();

    /**
     * current alive replication channels
     */
    private LinkedHashMap<String, List<SocketChannel>> aliveReplicationChannels = new LinkedHashMap<String, List<SocketChannel>>();

    /**
     * record unconnections
     */
    private ConcurrentHashMap<String, Integer> badChannels = new ConcurrentHashMap<String, Integer>();

    /**
     * server host
     */
    private static String SERVER_HOST = TemplateUtils.getMessage("server.host");

    /**
     * server port
     */
    private static int SERVER_PORT = Integer.parseInt(TemplateUtils.getMessage("server.port"));

    /**
     * shards
     */
    private static int SHAREDS = 0;

    private static final int MAX_SIZE = 10000;

    private DFSServer() {

    }

    public static synchronized DFSServer getInstance() {
        if (server == null) {
            server = new DFSServer();
        }
        return server;
    }

    public static void main(String[] args) {
        if (args != null && args.length > 0) {
            SHAREDS = Integer.parseInt(args[0].replace("-shareds=", ""));
        }
        Timer timer = new Timer();

        timer.schedule(new DFSObserver(), 5000, TIME);
        DFSServer dfsServer = DFSServer.getInstance();
        try {
            dfsServer.init();
            dfsServer.listen();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * init the channel and bind the host
     * 
     * @throws Exception
     */
    private void init() throws Exception {
        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.socket().bind(new InetSocketAddress(SERVER_HOST, SERVER_PORT));
        selector = Selector.open();
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);
    }

    /**
     * listen events
     */
    private void listen() throws Exception {
        System.out.println("server start");
        while (true) {
            selector.select();
            Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
            while (keys.hasNext()) {
                SelectionKey key = keys.next();
                if (key.isAcceptable()) {
                    keys.remove();

                    // client request to accpect
                    ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
                    SocketChannel channel = serverSocketChannel.accept();
                    channel.configureBlocking(false);
                    channel.register(selector, SelectionKey.OP_READ);
                    System.out.println("server receive client request " + channel.socket().getRemoteSocketAddress().toString());
                    
                    // judge current connected channels is saturated
                    if (sharedChannels.size() < SHAREDS) {
                        String k = channel.socket().getRemoteSocketAddress().toString();
                        sharedChannels.put(k, channel);
                    } else {
                        List<String> sharedKeys = new ArrayList(sharedChannels.keySet());
                        if (replicationChannels.size() == 0) {

                            // hasn't a replication
                            List<SocketChannel> reList = new ArrayList<SocketChannel>();
                            reList.add(channel);
                            replicationChannels.put(sharedKeys.get(0), reList);
                        } else {
                            boolean allHasRepli = true;
                            for (String sharedKey : sharedKeys) {
                                if (!replicationChannels.containsKey(sharedKey)) {

                                    // current shared has not replication
                                    allHasRepli = false;
                                    List<SocketChannel> reList = new ArrayList<SocketChannel>();
                                    reList.add(channel);
                                    replicationChannels.put(sharedKey, reList);
                                    break;
                                }
                            }

                            // all shared has replication
                            if (allHasRepli) {

                                // judge every replication size
                                String smallSharedKey = "";
                                int smallSize = MAX_SIZE;
                                for (String sharedKey : sharedKeys) {
                                    List<SocketChannel> chs = replicationChannels.get(sharedKey);
                                    if (chs != null) {
                                        if (chs.size() < smallSize) {
                                            smallSize = chs.size();
                                            smallSharedKey = sharedKey;
                                        }
                                    }
                                }
                                List<SocketChannel> chs = replicationChannels.get(smallSharedKey);
                                if (chs != null) {
                                    chs.add(channel);
                                }
                            }
                        }
                    }
                } else if (key.isReadable()) {

                    dispatchHandler((SocketChannel) key.channel());
                    key.interestOps(SelectionKey.OP_READ);
                } else if (key.isWritable()) {
                    System.out.println("write+++++++++++++++++++++++++++");
                }
            }
        }
    }

    /**
     * handle the receive data
     * 
     * @see com.zhy.easydfs.constants.Code
     * 
     * @param channel
     */
    private void dispatchHandler(SocketChannel channel) throws Exception {

        // the code capacity 100 bytes
        int capacity = 100;
        byte[] fileBytes = new byte[100];
        String code = "";
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        if (channel.read(buffer) > 0) {
            buffer.flip();
            buffer.get(fileBytes);
            buffer.clear();
        }
        code = new String(fileBytes).trim();
        System.out.println("c=" + code);
        if (code.length() > 0) {
            System.out.println("code==================" + code);
            if (NumberUtils.isInteger(code)) {

                // is sign code
                Code sign = Code.codeConvert(Integer.parseInt(code));
                codeHandler(sign, channel);
            } else {

                // is file
                File file = fileHandler(channel);
                write(file);

                // shard and replication
                int hashCode = file.hashCode();
                int shard = hashCode % sharedChannels.size();
                List<String> sharedKeys = new ArrayList(sharedChannels.keySet());
                String currentKey = sharedKeys.get(shard);
                SocketChannel currentChannel = sharedChannels.get(currentKey);
                currentChannel.register(selector, SelectionKey.OP_READ);

                // send shared channel
                send(file, currentChannel);

                // send the shared channel replication
                List<SocketChannel> replications = replicationChannels.get(currentKey);
                for (SocketChannel socketChannel : replications) {
                    send(file, socketChannel);
                }
            }

        }

    }

    /**
     * code handler
     * 
     * @param sign
     * @param channel
     */
    private void codeHandler(Code sign, SocketChannel channel) throws Exception {
        switch (sign) {

        // client return message
        case CLIENT_HEARTBEAT:
            // in alive channels
            String key = channel.socket().getRemoteSocketAddress().toString();
            System.out.println("server receive " + key);
            if (sharedChannels.containsKey(key)) {
                aliveSharedChannels.put(key, channel);
            } else if (replicationChannels.containsKey(key)) {
                List<SocketChannel> chs = aliveReplicationChannels.get(key);
                if (chs == null) {
                    chs = new ArrayList<SocketChannel>();
                }
                chs.add(channel);
                aliveReplicationChannels.put(key, chs);
            } else {
                System.out.println(key + " need close.");
            }

            break;
        case SERVER_ACCPECT_SUCCESS:
            break;
        default:
            break;
        }
    }

    /**
     * send file to shard
     * 
     * @param file
     */
    private void send(File file, SocketChannel channel) throws Exception {
        String fileName = file.getFileName();
        StringBuilder builder = new StringBuilder();
        if (fileName.length() < 100) {
            for (int i = 0; i < 100 - fileName.length(); i++) {
                builder.append(" ");
            }
        }
        String fileAllName = fileName + builder.toString();
        byte[] fileNameBytes = fileAllName.getBytes();
        byte[] contents = file.getContents();
        byte[] data = new byte[fileNameBytes.length + contents.length];
        for (int i = 0; i < fileNameBytes.length; i++) {
            data[i] = fileNameBytes[i];
        }
        int index = 0;
        for (int i = fileNameBytes.length; i < data.length; i++) {
            data[i] = contents[index];
            index++;
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        channel.write(buffer);
    }

    /**
     * persistence the data
     * 
     * @param file
     */
    private void write(File file) throws Exception {
        FileOutputStream fos = new FileOutputStream(new java.io.File(TemplateUtils.getMessage("server.file.path") + file.getFileName()));
        FileChannel fileChannel = fos.getChannel();
        ByteBuffer buffer = ByteBuffer.wrap(file.getContents());
        fileChannel.write(buffer);
        fileChannel.close();
        fos.close();
    }

    /**
     * receive and handle the data
     * 
     * @param channel
     * @throws Exception
     */
    private File fileHandler(SocketChannel channel) throws Exception {

        // sign the file name capacity 100 bytes
        int capacity = 100;
        byte[] fileBytes = new byte[100];
        String fileName = "";
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        if (channel.read(buffer) > 0) {
            buffer.flip();
            buffer.get(fileBytes);
            buffer.clear();
        }
        fileName = new String(fileBytes).trim();

        ByteBuffer dataBuffer = ByteBuffer.allocate(1024);
        int contentLength = 0;
        int size = -1;
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        while ((size = channel.read(dataBuffer)) > 0) {
            contentLength += size;
            dataBuffer.flip();
            dataBuffer.limit(size);
            byteArrayOutputStream.write(dataBuffer.array());
            dataBuffer.clear();
        }
        // channel.close();
        byteArrayOutputStream.close();
        byte[] byteArray = byteArrayOutputStream.toByteArray();
        return new File(fileName, contentLength, byteArray);
    }

    protected void clearAlivedChannels() {
        aliveReplicationChannels.clear();
        aliveSharedChannels.clear();
    }

    /**
     * alive channels to used NOTICE: MUST wait the result receivce
     */
    protected synchronized void alivedToUsed() {
        List<String> keys = new ArrayList(sharedChannels.keySet());
        for (String key : keys) {
            if (!aliveSharedChannels.containsKey(key)) {
                if (badChannels.containsKey(key)) {
                    int badConnectionCount = badChannels.get(key) + 1;
                    if (badConnectionCount >= 3) {
                        sharedChannels.remove(key);
                    } else {
                        badChannels.put(key, badConnectionCount);
                        System.out.println("bad connection is " + key + " count :" + badConnectionCount);
                    }
                } else {
                    badChannels.put(key, 1);
                    System.out.println("bad connection is " + key + " count :1");
                }
            }
        }
        System.out.println("current shareds size:" + sharedChannels.size());
        
        
        
        List<String> ks = new ArrayList(replicationChannels.keySet());
        List<SocketChannel> repliChannels = new ArrayList(aliveReplicationChannels.values());
        Map<String, SocketChannel> tempMap = new LinkedHashMap<String, SocketChannel>();
        for(SocketChannel channel : repliChannels) {
            tempMap.put(channel.socket().getRemoteSocketAddress().toString(), channel);
        }
        for(String key : ks ) {
            if(!tempMap.containsKey(key)) {
                if (badChannels.containsKey(key)) {
                    int badConnectionCount = badChannels.get(key) + 1;
                    if (badConnectionCount >= 3) {
                        Iterator<String> kk = replicationChannels.keySet().iterator();
                        while(kk.hasNext()) {
                            String k = kk.next();
                            List<SocketChannel> channels = replicationChannels.get(k);
                            for(SocketChannel chl : channels) {
                                if(chl.socket().getRemoteSocketAddress().toString().equals(key)) {
                                    channels.remove(chl);
                                    break;
                                }
                            }
                        }
                    } else {
                        badChannels.put(key, badConnectionCount);
                        System.out.println("bad connection is " + key + " count :" + badConnectionCount);
                    }
                } else {
                    badChannels.put(key, 1);
                    System.out.println("bad connection is " + key + " count :1");
                }
            }
        }
        
        System.out.println("replication size:" + ks.size());
        // while (keys.hasNext()) {
        // String key = keys.next();
        // System.out.println(key);
        // }

        // while (ks.hasNext()) {
        // String key = ks.next();
        // List<SocketChannel> channels = replicationChannels.get(key);
        // if (channels != null) {
        // for (SocketChannel c : channels) {
        // System.out.println(c.socket().getRemoteSocketAddress().toString());
        // }
        // }
        // }
        clearAlivedChannels();
    }

    /**
     * boardCast every channel
     * 
     * @throws Exception
     */
    protected void boardCast() throws Exception {
        String text = String.valueOf(Code.SERVER_HEARTBEAT.getCode());
        StringBuilder builder = new StringBuilder();
        if (text.length() < 100) {
            for (int i = 0; i < 100 - text.length(); i++) {
                builder.append(" ");
            }
        }
        text = text + builder.toString();
        List<String> sharedKeys = new ArrayList(sharedChannels.keySet());
        for (String sharedKey : sharedKeys) {
            SocketChannel sharedChannel = sharedChannels.get(sharedKey);
            if (sharedChannel != null) {
                try {
                    System.out.println("boardcast to " + sharedKey);
                    sharedChannel.write(ByteBuffer.wrap(text.getBytes()));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                List<SocketChannel> sharedRepliChannels = replicationChannels.get(sharedKey);
                if (sharedRepliChannels != null && !sharedRepliChannels.isEmpty()) {
                    for (SocketChannel sharedRepliChannel : sharedRepliChannels) {
                        try {
                            sharedRepliChannel.write(ByteBuffer.wrap(text.getBytes()));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }

    protected LinkedHashMap<String, SocketChannel> getSharedChannels() {
        return sharedChannels;
    }

    protected void setSharedChannels(LinkedHashMap<String, SocketChannel> sharedChannels) {
        this.sharedChannels = sharedChannels;
    }

    protected Map<String, List<SocketChannel>> getReplicationChannels() {
        return replicationChannels;
    }

    protected void setReplicationChannels(LinkedHashMap<String, List<SocketChannel>> replicationChannels) {
        this.replicationChannels = replicationChannels;
    }

    protected Selector getSelector() {
        return selector;
    }

    protected void setSelector(Selector selector) {
        this.selector = selector;
    }
}
