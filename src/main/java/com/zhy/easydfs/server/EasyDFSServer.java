package com.zhy.easydfs.server;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
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

import com.zhy.easydfs.constants.Code;
import com.zhy.easydfs.file.File;
import com.zhy.easydfs.util.ArrayUtils;
import com.zhy.easydfs.util.ChannelUtils;
import com.zhy.easydfs.util.NumberUtils;
import com.zhy.easydfs.util.StringUtils;
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
     * shared file text
     */
    private Map<Integer, List<String>> sharedIndexed = new ConcurrentHashMap<Integer, List<String>>();

    /**
     * download channels<br>
     * key:download file name
     */
    private Map<String, List<SocketChannel>> downloadChannels = new ConcurrentHashMap<String, List<SocketChannel>>();

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

    private EasyDFSServer() {

    }

    public static synchronized EasyDFSServer getInstance() {
        if (server == null) {
            server = new EasyDFSServer();
        }
        return server;
    }

    public static void main(String[] args) {
        if(args == null || args.length < 2) {
            System.err.println("Please input the shareds and server folder.");
            System.exit(-1);
        }
        if (args != null && args.length > 0) {
            SHAREDS = Integer.parseInt(args[0].replace("-shareds=", ""));
        }
        if (args != null && args.length > 1) {
            SERVER_FOLDER = args[1].replace("-folder=", "");
        }
        Timer timer = new Timer();

        timer.schedule(new EasyDFSObserver(), 5000, TIME);
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
        initFileText();
    }

    /**
     * init server index file
     */
    private void initFileText() {
        java.io.File folder = new java.io.File(SERVER_FOLDER);
        java.io.File[] files = folder.listFiles(new FilenameFilter() {

            @Override
            public boolean accept(java.io.File dir, String name) {
                if ((dir.getAbsolutePath() + "\\").equals(SERVER_FOLDER) && name.contains(".index")) {
                    return true;
                }
                return false;
            }
        });
        if (files != null) {
            for (java.io.File f : files) {
                String fileName = f.getName();
                String prefix = fileName.substring(0, fileName.indexOf("."));
                BufferedReader reader = null;
                try {
                    reader = new BufferedReader(new InputStreamReader(new FileInputStream(f.getAbsoluteFile())));
                    String str = null;
                    List<String> text = new ArrayList<String>();
                    while ((str = reader.readLine()) != null) {
                        text.add(str);
                    }
                    sharedIndexed.put(Integer.parseInt(prefix), text);
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        reader.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

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
                    System.out.println("Server receive client connection request " + channel.socket().getRemoteSocketAddress().toString());
                } else if (key.isReadable()) {
                    try {
                        dispatchHandler(key);
                    } catch (Exception e) {
                        e.printStackTrace();
                        key.cancel();
                    }
                } else {
                    System.out.println("other events?");
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
            System.out.println("As shared channel: " + channel.socket().getRemoteSocketAddress());
            sharedChannels.put(k, channel);
        } else {
            List<String> sharedKeys = new ArrayList(sharedChannels.keySet());
            if (replicationChannels.size() == 0) {

                // hasn't a replication
                List<SocketChannel> reList = new ArrayList<SocketChannel>();
                reList.add(channel);
                String sharedKey = sharedKeys.get(0);
                System.out.println("As replication channel: " + channel.socket().getRemoteSocketAddress() + " of " + sharedKey + " shared channel---");
                replicationChannels.put(sharedKey, reList);
            } else {
                boolean allHasRepli = true;
                for (String sharedKey : sharedKeys) {
                    if (!replicationChannels.containsKey(sharedKey)) {

                        // current shared has not replication
                        allHasRepli = false;
                        List<SocketChannel> reList = new ArrayList<SocketChannel>();
                        reList.add(channel);
                        System.out.println("As replication channel: " + channel.socket().getRemoteSocketAddress() + " of " + sharedKey + " shared channel---");
                        replicationChannels.put(sharedKey, reList);
                        break;
                    }
                }

                // all shared has replication
                if (allHasRepli) {

                    // judge every replication size
                    String smallSharedKey = "";
                    int smallSize = Integer.MAX_VALUE;
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
                        System.out.println("As replication channel: " + channel.socket().getRemoteSocketAddress() + " of " + smallSharedKey + " shared channel---");
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
            String code = ChannelUtils.readTop100(channel);
            System.out.println("Server receive code: " + code);
            if (code.length() > 0) {
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

                    // create file and get version
                     String version = createVersionFile(file.getFileName(), shard);
                     
                    // send shared channel
                    send(file, version, currentChannel);
                    System.out.println("Sended to shared :" + currentKey);

                    // send the shared channel replication
//                    List<SocketChannel> replications = replicationChannels.get(currentKey);
//                    if (replications != null) {
//                        for (SocketChannel socketChannel : replications) {
//                            send(file,version, socketChannel);
//                            System.out.println("Sended to replication :" + socketChannel.socket().getRemoteSocketAddress());
//                        }
//                    }

                    // send upload success identification
                    send(Code.OPT_UPLOAD_SUCCESS, channel);
                    channel.close();
                    createIndexFile(shard, file.getFileName());
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception(e);
        }

    }

    /**
     * start other using append file
     * 
     * @param shard
     * @param fileName
     */
    private void createIndexFile(int shard, String fileName) {
        String sharedIndexFilePath = SERVER_FOLDER + shard + ".index";
        String str = fileName + "\r\n";
        Thread appendIndexFileThread = new Thread(new FileOpt(new java.io.File(sharedIndexFilePath), str));
        appendIndexFileThread.start();
    }

    /**
     * create version file
     * 
     * @param fileName
     * @return current version 
     */
    private String createVersionFile(String fileName, int shared) {
        String time = String.valueOf(System.currentTimeMillis()).substring(0, 10);
        String file = shared + ".version";
        StringBuilder builder = new StringBuilder();
        String version = "V." + time;
        builder.append(version + "\r\n");
        builder.append("A " + fileName + "\r\n");
        String text = builder.toString();
        new Thread(new FileOpt(new java.io.File(SERVER_FOLDER + file), text)).start();
        return version;
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
        case OPT_DOWNLOAD:
            handlerDownload(channel);
        case OPT_DOWNLOAD_FOUND_FILE:
            handlerDownloadOver(channel);
        default:
            break;
        }
    }

    /**
     * handler the client message
     * 
     * @param channel
     */
    private void handlerDownloadOver(SocketChannel channel) throws Exception {
        String fileName = ChannelUtils.readTop100(channel);
        String length = ChannelUtils.readTop100(channel);

        if (NumberUtils.isInteger(length)) {

            byte[] byteArray = ChannelUtils.readFile(channel, Integer.parseInt(length));
            if (downloadChannels.containsKey(fileName)) {

                byte[] returnCode = StringUtils.fullSpace(Code.OPT_DOWNLOAD_SUCCESS.getCode()).getBytes();
                byte[] fileNameBytes = StringUtils.fullSpace(fileName).getBytes();
                byte[] fileContent = byteArray;
                byte[] fileLength = StringUtils.fullSpace(fileContent.length).getBytes();
                byte[] array = new byte[returnCode.length + fileNameBytes.length + fileLength.length + fileContent.length];

                // sequence code, filename, length, content
                ArrayUtils.arrayBytesCopy(returnCode, array, 0);
                ArrayUtils.arrayBytesCopy(fileNameBytes, array, returnCode.length);
                ArrayUtils.arrayBytesCopy(fileLength, array, returnCode.length + fileNameBytes.length);
                ArrayUtils.arrayBytesCopy(fileContent, array, returnCode.length + fileNameBytes.length + fileLength.length);

                // Send to all of the channels want to download files
                List<SocketChannel> chs = downloadChannels.get(fileName);
                for (SocketChannel c : chs) {
                    c.write(ByteBuffer.wrap(array));
                    c.socket().shutdownOutput();
                }

            }
        }
    }

    /**
     * control file download
     * 
     * @param channel
     */
    private void handlerDownload(SocketChannel channel) throws Exception {
        String fileName = ChannelUtils.readTop100(channel);
        File file = new File();

        // only calculate filename hashcode
        file.setFileName(fileName);
        int sharedCode = file.hashCode();
        if (sharedChannels.size() > 0) {
            sharedCode = sharedCode % sharedChannels.size();
            List<String> fileNames = sharedIndexed.get(sharedCode);
            int indexChannel = -1;
            if (fileNames != null) {
                for (String f : fileNames) {
                    if (f.equals(fileName)) {
                        indexChannel = sharedCode;
                        break;
                    }
                }
            }

            // Could not Find the file in the current chared
            if (indexChannel == -1) {

            }

            // find the file in client
            if (indexChannel != -1) {
                if (indexChannel <= sharedChannels.size()) {
                    List<String> keys = new ArrayList<String>(sharedChannels.keySet());
                    String key = keys.get(indexChannel);
                    SocketChannel thisChannel = sharedChannels.get(key);
                    String codeStr = StringUtils.fullSpace(Code.OPT_DOWNLOAD_REQUEST_CLIENT.getCode());
                    String fileStr = StringUtils.fullSpace(fileName);
                    byte[] data = StringUtils.mergeAndGenerateBytes(codeStr, fileStr);
                    thisChannel.write(ByteBuffer.wrap(data));

                    if (downloadChannels.get(fileName) != null) {
                        List<SocketChannel> chs = downloadChannels.get(fileName);
                        chs.add(channel);
                        downloadChannels.put(fileName, chs);
                    } else {
                        List<SocketChannel> chs = new ArrayList<SocketChannel>();
                        chs.add(channel);
                        downloadChannels.put(fileName, chs);
                    }
                }
            }
        } else {

            // write unreadable error code and close the channel

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
        System.out.println("Keepalive channel: " + key);
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
    private void send(File file, String version, SocketChannel channel) throws Exception {
        String fileName = file.getFileName();
        byte[] fileNameBytes = StringUtils.fullSpace(fileName).getBytes();
        byte[] versionBytes = StringUtils.fullSpace(version).getBytes();
        byte[] contents = file.getContents();
        byte[] lengthBytes = StringUtils.fullSpace(file.getContents().length).getBytes();
        byte[] array = new byte[fileNameBytes.length + versionBytes.length + lengthBytes.length + contents.length];
        
        ArrayUtils.arrayBytesCopy(fileNameBytes, array, 0);
        ArrayUtils.arrayBytesCopy(versionBytes, array, fileNameBytes.length);
        ArrayUtils.arrayBytesCopy(lengthBytes, array, versionBytes.length + fileNameBytes.length);
        ArrayUtils.arrayBytesCopy(contents, array, versionBytes.length + fileNameBytes.length + lengthBytes.length);
        channel.write(ByteBuffer.wrap(array));
        channel.register(selector, SelectionKey.OP_READ);
    }

    /**
     * send code
     * 
     * @param code
     * @param channel
     * @throws Exception
     */
    private void send(Code code, SocketChannel channel) throws Exception {
        channel.write(ByteBuffer.wrap(StringUtils.fullSpace(code.getCode()).getBytes()));
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
        ByteBuffer dataBuffer = ByteBuffer.allocateDirect(1024);
        int contentLength = 0;
        int size = -1;
        byte[] bytes = null;
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        while ((size = channel.read(dataBuffer)) != -1) {
            contentLength += size;
            dataBuffer.flip();
            bytes = new byte[size];
            dataBuffer.get(bytes);
            byteArrayOutputStream.write(bytes);
            dataBuffer.clear();
        }
        byte[] byteArray = byteArrayOutputStream.toByteArray();
        byteArrayOutputStream.close();
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
            for (int i = 0; i < keys.size(); i++) {
                String key = keys.get(i);
                if (!aliveSharedChannels.containsKey(key)) {
                    if (badChannels.containsKey(key)) {
                        int badConnectionCount = badChannels.get(key) + 1;
                        if (badConnectionCount >= 3) {
                            sharedChannels.remove(key);
                            
                            // replication instead of shared
                            List<SocketChannel> replis = replicationChannels.get(key);
                            if(replis != null && !replis.isEmpty()) {
                                for(SocketChannel s : replis) {
                                    String currentRepliKey = s.socket().getRemoteSocketAddress().toString();
                                    if(badChannels.containsKey(currentRepliKey)) {
                                        
                                        // judge if the this replication was a bad channel but the bad count < 2 is valid
                                        if(badChannels.get(currentRepliKey) < 2) {
                                           
                                            // replace the shared
                                            LinkedHashMap<String, SocketChannel> insteadSharedChannels = new LinkedHashMap<String, SocketChannel>();
                                            for(int j = 0; j < keys.size(); j++) {
                                                String k = keys.get(j);
                                                if(j == i) {
                                                    
                                                    // instead bad channel
                                                    insteadSharedChannels.put(currentRepliKey, s);
                                                } else {
                                                    insteadSharedChannels.put(k, sharedChannels.get(k));
                                                }
                                            }
                                            sharedChannels = insteadSharedChannels;
                                            
                                            // Remove old key and set new key
                                            System.out.println("replication ---> " + currentRepliKey + " Instead of " + key);
                                            List<SocketChannel> oldSharedChannels = replicationChannels.remove(key);
                                            replicationChannels.put(currentRepliKey, oldSharedChannels);
                                            break;
                                        }
                                    } else {
                                        
                                        // this replication is avaliable
                                        // replace the shared
                                        LinkedHashMap<String, SocketChannel> insteadSharedChannels = new LinkedHashMap<String, SocketChannel>();
                                        for (int j = 0; j < keys.size(); j++) {
                                            String k = keys.get(j);
                                            if (j == i) {

                                                // instead bad channel
                                                insteadSharedChannels.put(currentRepliKey, s);
                                            } else {
                                                insteadSharedChannels.put(k, sharedChannels.get(k));
                                            }
                                        }
                                        sharedChannels = insteadSharedChannels;

                                        // Remove old key and set new key
                                        System.out.println("replication ---> " + currentRepliKey + " Instead of " + key);
                                        List<SocketChannel> oldSharedChannels = replicationChannels.remove(key);
                                        replicationChannels.put(currentRepliKey, oldSharedChannels);
                                        break;
                                    }
                                }
                            } else {
                                
                                // the shared no replis maybe current server should be down
                                System.out.println("need down");
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
        byte[] text = StringUtils.fullSpace(Code.SERVER_HEARTBEAT.getCode()).getBytes();
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
                    System.out.println("Boardcast to " + sharedKey);
                    sharedChannel.write(ByteBuffer.wrap(text));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                List<SocketChannel> sharedRepliChannels = replicationChannels.get(sharedKey);
                if (sharedRepliChannels != null && !sharedRepliChannels.isEmpty()) {
                    for (SocketChannel sharedRepliChannel : sharedRepliChannels) {
                        try {
                            sharedRepliChannel.write(ByteBuffer.wrap(text));
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
