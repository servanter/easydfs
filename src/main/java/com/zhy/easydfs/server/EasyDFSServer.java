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
import java.sql.Timestamp;
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
public class EasyDFSServer {

    /**
     * every five minutes
     */
    public static final int TIME = 1000 * 10;

    private static EasyDFSServer server = null;

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
     * server save folder
     */
    private static String SERVER_FOLDER;

    /**
     * shards
     */
    private static int SHAREDS = 0;

    private static final int MAX_SIZE = 10000;

    private EasyDFSServer() {

    }

    public static synchronized EasyDFSServer getInstance() {
        if (server == null) {
            server = new EasyDFSServer();
        }
        return server;
    }

    public static void main(String[] args) {
        if (args != null && args.length > 0) {
            SHAREDS = Integer.parseInt(args[0].replace("-shareds=", ""));
        }
        if (args != null && args.length > 0) {
            SERVER_FOLDER = args[1].replace("-folder=", "");
        }
        Timer timer = new Timer();

         timer.schedule(new EasyDFSObserver(), 5000*6, TIME);
        EasyDFSServer dfsServer = EasyDFSServer.getInstance();
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

                    // when client request to download file, write file and close the channel
                    // TODO
                    // channel.write(ByteBuffer.wrap("aab".getBytes()));
                    // channel.close();
                } else if (key.isReadable()) {
                    try {
                        dispatchHandler(key);
                    } catch (Exception e) {
                        e.printStackTrace();
                        key.cancel();
                    }
                } else {
                    System.out.println("cccccccccccccccccccccccccccc");
                }
            }
        }
    }

    /**
     * add channel in shared or replication
     * 
     * @param channel
     */
    public void addChannel(SocketChannel channel) {

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
    }

    /**
     * handle the receive data
     * 
     * @see com.zhy.easydfs.constants.Code
     * 
     * @param channel
     */
    private void dispatchHandler(SelectionKey key) throws Exception {
        SocketChannel channel = (SocketChannel) key.channel();

        try {

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
                    File file = fileHandler(code, channel);
                    write(file, SERVER_FOLDER);

                    // shard and replication
                    int hashCode = file.hashCode();
                    int shard = hashCode % sharedChannels.size();
                    List<String> sharedKeys = new ArrayList(sharedChannels.keySet());
                    String currentKey = sharedKeys.get(shard);
                    SocketChannel currentChannel = sharedChannels.get(currentKey);
                    currentChannel.register(selector, SelectionKey.OP_READ);

                    // create version
                    File version = createVersionFile(file.getFileName(), currentKey);

                    // send shared channel version
                    // send(version, currentChannel);

                    // send shared channel
                    send(file, currentChannel);

                    // send the shared channel replication
                    List<SocketChannel> replications = replicationChannels.get(currentKey);
                    if (replications != null) {
                        for (SocketChannel socketChannel : replications) {
                            send(file, socketChannel);
                        }
                    }
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception(e);
        }

    }

    /**
     * create version file
     * 
     * @param fileName
     * @return
     */
    private File createVersionFile(String fileName, String shared) {
        String time = String.valueOf(System.currentTimeMillis()).substring(0, 10);
        File file = new File();
        String content = "fileName:" + fileName + "\r\n" + "version:" + time;
        file.setContents(content.getBytes());
        file.setFileName("version");
        return file;
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
            handlerAlive(channel);
            break;
        case SYSTEM_CHANNEL:

            // as system channels
            addChannel(channel);

            // in alive channels
            handlerAlive(channel);
            break;
        default:
            break;
        }
    }

    /**
     * handle current alive channel
     * 
     * @param channel
     */
    private void handlerAlive(SocketChannel channel) {
        List<String> replisKeys = new ArrayList<String>();
        Iterator<List<SocketChannel>> scs = replicationChannels.values().iterator();
        while (scs.hasNext()) {
            List<SocketChannel> everys = scs.next();
            for (SocketChannel c : everys) {
                replisKeys.add(c.socket().getRemoteSocketAddress().toString());
            }
        }

        // in alive channels
        String key = channel.socket().getRemoteSocketAddress().toString();
        System.out.println("server receive " + key);
        if (sharedChannels.containsKey(key)) {
            aliveSharedChannels.put(key, channel);
        } else if (replisKeys.contains(key)) {
            List<SocketChannel> chs = aliveReplicationChannels.get(key);
            if (chs == null) {
                chs = new ArrayList<SocketChannel>();
            }
            chs.add(channel);
            aliveReplicationChannels.put(key, chs);
        } else {
            System.out.println(key + " need close.");
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
    private void write(File file, String folder) throws Exception {
        FileOutputStream fos = new FileOutputStream(new java.io.File(folder + file.getFileName()));
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
    private File fileHandler(String fileName, SocketChannel channel) throws Exception {
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
        synchronized (serverChannel) {

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

            List<String> replisKeys = new ArrayList<String>();
            Iterator<List<SocketChannel>> scs = replicationChannels.values().iterator();
            while (scs.hasNext()) {
                List<SocketChannel> everys = scs.next();
                for (SocketChannel c : everys) {
                    replisKeys.add(c.socket().getRemoteSocketAddress().toString());
                }
            }

            Map<String, SocketChannel> tempMap = new LinkedHashMap<String, SocketChannel>();
            Iterator<List<SocketChannel>> repliChannels = aliveReplicationChannels.values().iterator();
            while (repliChannels.hasNext()) {
                List<SocketChannel> rs = repliChannels.next();
                for (SocketChannel channel : rs) {
                    tempMap.put(channel.socket().getRemoteSocketAddress().toString(), channel);
                }
            }
            for (String key : replisKeys) {
                if (!tempMap.containsKey(key)) {
                    if (badChannels.containsKey(key)) {
                        int badConnectionCount = badChannels.get(key) + 1;
                        if (badConnectionCount >= 3) {
                            Iterator<String> kk = replicationChannels.keySet().iterator();
                            while (kk.hasNext()) {
                                String k = kk.next();
                                List<SocketChannel> channels = replicationChannels.get(k);
                                for (SocketChannel chl : channels) {
                                    if (chl.socket().getRemoteSocketAddress().toString().equals(key)) {
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

            System.out.println("replication size:" + replisKeys.size());
            clearAlivedChannels();
        }
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

        List<SelectionKey> selects = new ArrayList<SelectionKey>(selector.keys());
        for (SelectionKey key : selects) {
            if (key.channel() instanceof SocketChannel) {
                key.interestOps(SelectionKey.OP_READ);
            }
        }

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