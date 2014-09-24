package com.zhy.easydfs.server;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
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
import com.zhy.easydfs.constants.Constants;
import com.zhy.easydfs.file.File;
import com.zhy.easydfs.util.ArrayUtils;
import com.zhy.easydfs.util.ChannelUtils;
import com.zhy.easydfs.util.FileUtils;
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
        if (args == null || args.length < 2) {
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
        timer.schedule(new ReplicaCollect(), 10000, 1000 * 5);
        timer.schedule(new RepliSync(), 20000, TIME);
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

                // add new file index
                if (!sharedIndexed.containsKey(prefix)) {
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
                if (key.channel().isOpen() && key.isAcceptable()) {
                    keys.remove();

                    // client request to accpect
                    ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
                    SocketChannel channel = serverSocketChannel.accept();
                    channel.configureBlocking(false);
                    channel.register(selector, SelectionKey.OP_READ);
                    System.out.println("Server receive client connection request " + channel.socket().getRemoteSocketAddress().toString());
                } else if (key.channel().isOpen() && key.isReadable()) {
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
                System.out
                        .println("As replication channel: " + channel.socket().getRemoteSocketAddress() + " of " + sharedKey + " shared channel---");
                replicationChannels.put(sharedKey, reList);
            } else {
                boolean allHasRepli = true;
                for (String sharedKey : sharedKeys) {
                    if (!replicationChannels.containsKey(sharedKey)) {

                        // current shared has not replication
                        allHasRepli = false;
                        List<SocketChannel> reList = new ArrayList<SocketChannel>();
                        reList.add(channel);
                        System.out.println("As replication channel: " + channel.socket().getRemoteSocketAddress() + " of " + sharedKey
                                + " shared channel---");
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
                        System.out.println("As replication channel: " + channel.socket().getRemoteSocketAddress() + " of " + smallSharedKey
                                + " shared channel---");
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
                    // write(file, SERVER_FOLDER);

                    // shard and replication
                    int hashCode = file.hashCode();
                    int shared = hashCode % sharedChannels.size();
                    List<String> sharedKeys = new ArrayList(sharedChannels.keySet());
                    String currentKey = sharedKeys.get(shared);
                    SocketChannel currentChannel = sharedChannels.get(currentKey);
                    currentChannel.register(selector, SelectionKey.OP_READ);

                    // send shared channel
                    send(file, currentChannel);
                    System.out.println("Sended to shared :" + currentKey);

                    // send upload success identification
                    send(Code.OPT_UPLOAD_SUCCESS, channel);
                    channel.close();

                    // file index list only store cache, the index file is not necessary store file
                    fileNameInCache(shared, file.getFileName());
                    // createIndexFile(shard, file.getFileName());
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception(e);
        }

    }

    /**
     * Upload file name in cache. Must wait for the shared file list return in the future
     * 
     * @param shared
     * @param fileName
     */
    private void fileNameInCache(int shared, String fileName) {
        if (sharedIndexed.containsKey(shared)) {
            sharedIndexed.get(shared).add(fileName);
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
            break;
        case OPT_DOWNLOAD_FOUND_FILE:
            handlerDownloadOver(channel);
            break;
        case SERVER_SYNC_SHARED_INDEX_FILE_SUCCESS:
            handlerSharedSync(channel);
            break;
        case SERVER_SYNC_SHARED_INDEX_FILE_EMPTY:
            handlerSharedSyncEmpty(channel);
            break;
        case REPLICATION_SYNC_SHARED:
            handlerRepliSync(channel);
            break;
        case SHARED_RECEIVE_SYNC_AND_SEND_REPLICATION:
            handlerRepliSyncServerReceive(channel);
            break;
        case REPLICATION_RECEIVE_SHARED_SUCCESS:
            handlerRepliSyncSuccess(channel);
        default:
            break;
        }
    }

    /**
     * the replication receive the own shared channel send file
     * 
     * @param channel
     */
    private void handlerRepliSyncSuccess(SocketChannel channel) {
        try {
            String version = ChannelUtils.readTop100(channel);
            String alias = ChannelUtils.getUniqueAlias(channel);

            String sharedKey = "";
            List<String> keys = new ArrayList<String>(replicationChannels.keySet());
            for (String key : keys) {
                List<SocketChannel> channels = replicationChannels.get(key);
                for (SocketChannel every : channels) {
                    if (ChannelUtils.getUniqueAlias(every).equals(alias)) {
                        sharedKey = key;
                        break;
                    }
                }
            }
            if (sharedKey.length() > 0 && sharedChannels.containsKey(sharedKey)) {
                int index = 0;
                List<String> sharedKeys = new ArrayList<String>(sharedChannels.keySet());
                for (String s : sharedKeys) {
                    if (s.equals(sharedKey)) {
                        break;
                    }
                    index++;
                }
                Map<String, String[]> versionMap = FileUtils.readFileListLine(SERVER_FOLDER + index + Constants.REPLICATION_VERSION_POST, " ");
                if (versionMap.containsKey(alias)) {
                    String arr[] = versionMap.get(alias);
                    arr[1] = version;
                    versionMap.put(alias, arr);
                }
                StringBuilder builder = new StringBuilder();
                List<String> mapKeys = new ArrayList<String>(versionMap.keySet());
                for (String key : mapKeys) {
                    String[] arr = versionMap.get(key);
                    for (String every : arr) {
                        builder.append(every + " ");
                    }
                    builder.append("\r\n");
                }
                FileUtils.writeFile(SERVER_FOLDER + index + Constants.REPLICATION_VERSION_POST, builder.toString().getBytes(), false);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * server receive client send sync message
     * 
     * @param channel
     */
    private void handlerRepliSyncServerReceive(SocketChannel channel) {
        try {

            byte[] returnCode = StringUtils.fullSpace(Code.SERVER_RECEIVE_SHARED_AND_SEND_TO_REPLICATION.getCode()).getBytes();
            String version = ChannelUtils.readTop100(channel);
            byte[] returnVersion = StringUtils.fullSpace(version).getBytes();
            byte[] instruction = StringUtils.fullSpace(ChannelUtils.readTop100(channel)).getBytes();
            byte[] fileNameBytes = StringUtils.fullSpace(ChannelUtils.readTop100(channel)).getBytes();
            String length = ChannelUtils.readTop100(channel);
            byte[] fileContent = ChannelUtils.readFile(channel, Integer.parseInt(length));
            byte[] fileLength = StringUtils.fullSpace(fileContent.length).getBytes();
            byte[] array = new byte[returnCode.length + returnVersion.length + instruction.length + fileNameBytes.length + fileLength.length
                    + fileContent.length];

            ArrayUtils.arrayBytesCopy(returnCode, array, 0);
            ArrayUtils.arrayBytesCopy(returnVersion, array, returnCode.length);
            ArrayUtils.arrayBytesCopy(instruction, array, returnCode.length + returnVersion.length);
            ArrayUtils.arrayBytesCopy(fileNameBytes, array, returnCode.length + returnVersion.length + instruction.length);
            ArrayUtils.arrayBytesCopy(fileLength, array, returnCode.length + returnVersion.length + instruction.length + fileNameBytes.length);
            ArrayUtils.arrayBytesCopy(fileContent, array, returnCode.length + returnVersion.length + instruction.length + fileNameBytes.length
                    + fileLength.length);

            String alias = ChannelUtils.getUniqueAlias(channel);
            int index = 0;
            List<String> keys = new ArrayList<String>(sharedChannels.keySet());
            for (String key : keys) {
                if (key.equals(alias)) {
                    break;
                }
                index++;
            }

            if (sharedChannels.containsKey(alias)) {
                List<SocketChannel> replis = replicationChannels.get(alias);
                Map<String, String[]> map = FileUtils.readFileListLine(SERVER_FOLDER + index + Constants.REPLICATION_VERSION_POST, " ");
                for (SocketChannel repli : replis) {
                    String key = ChannelUtils.getUniqueAlias(repli);
                    if (map.containsKey(key)) {

                        // the replica is new
                        String[] arr = map.get(key);
                        if (!arr[2].equals(Constants.REPLICATION_RUNNING)) {

                            // This channel is not running.
                            continue;
                        }
                        if (arr[1].equals(Constants.NO_VERSION)) {
                            ChannelUtils.write(repli, array);
                        } else {

                            // judge the replica version
                            String currentVersion = arr[1].replace("V.", "");
                            long currentVersionLong = Long.parseLong(currentVersion);
                            long receiveVersionLong = Long.parseLong(version.replace("V.", ""));
                            if (currentVersionLong < receiveVersionLong) {
                                ChannelUtils.write(repli, array);
                            }
                        }

                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * replication request sync
     * 
     * @param channel
     */
    private void handlerRepliSync(SocketChannel channel) {
        try {

            // find the replication location
            String alias = ChannelUtils.getUniqueAlias(channel);
            List<String> keys = new ArrayList<String>(replicationChannels.keySet());
            String sharedKey = "";
            int index = 0;
            loop: for (String key : keys) {
                List<SocketChannel> channels = replicationChannels.get(key);
                for (SocketChannel sch : channels) {
                    if (ChannelUtils.getUniqueAlias(sch).equals(alias)) {
                        sharedKey = key;
                        break loop;
                    }
                }
                index++;
            }
            if (sharedKey.length() > 0) {

                // record the replication version
                String currentRepliVersion = ChannelUtils.readTop100(channel);
                String repliVersion = index + Constants.REPLICATION_VERSION_POST;
                String content = alias + " " + currentRepliVersion + " " + Constants.REPLICATION_WAITING + "\r\n";
                String fileName = SERVER_FOLDER + repliVersion;

                // read the replica version file and write current replica version
                try {
                    Map<String, String[]> readLine = FileUtils.readFileListLine(fileName, " ");
                    if (readLine.containsKey(alias)) {

                        // the replica version has this channel
                        String[] array = readLine.get(alias);
                        array[1] = currentRepliVersion;
                    } else {

                        // doesn't has the channel, Add this channel info
                        String[] thisInfo = { alias, currentRepliVersion, Constants.REPLICATION_WAITING };
                        readLine.put(alias, thisInfo);
                    }
                    StringBuilder builder = new StringBuilder();
                    List<String> mapKeys = new ArrayList<String>(readLine.keySet());
                    for (String mapKey : mapKeys) {
                        String[] array = readLine.get(mapKey);
                        builder.append(array[0] + " " + array[1] + " " + array[2] + "\r\n");

                        // if this channel is not sync, So not to need find the small version, Becaues this is small
//                        if (!smallVersion.equals(Constants.NO_VERSION)) {
//
//                            // judge the small version
//                            if (array[1].equals(Constants.NO_VERSION)) {
//                                smallVersion = Constants.NO_VERSION;
//                            } else {
//                                String str = array[1].replace("V.", "");
//                                try {
//                                    int channelVer = Integer.parseInt(str);
//                                    int smallVersinInt = Integer.parseInt(smallVersion.replace("V.", ""));
//                                    if (smallVersinInt > channelVer) {
//                                        smallVersion = String.valueOf(channelVer);
//                                    }
//                                } catch (Exception e) {
//                                    e.printStackTrace();
//                                    System.err.println("The replica version is not valid ?");
//                                }
//                            }
//                        }
                    }
                    content = builder.toString();
                } catch (Exception e) {
                    System.err.println("Server create replica version occur error, Maybe the file not found.");
                }

                FileUtils.writeFile(fileName, content.getBytes(), false);
                System.out.println("Create the repli version file: " + fileName + " last replica version is: " + currentRepliVersion);

                // TODO need split twice parts
                // one store file status 0
                // one collect replica version find small version and modify status 1
//                SocketChannel sharedChannel = sharedChannels.get(sharedKey);
//
//                // send the code and the version to shared
//                if (sharedChannel != null) {
//                    String code = StringUtils.fullSpace(Code.REPLICATION_SYNC_SHARED.getCode());
//                    String version = StringUtils.fullSpace(currentRepliVersion);
//                    ChannelUtils.write(sharedChannel, new String(code + version));
//                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void handlerSharedSyncEmpty(SocketChannel channel) {
        try {
            String name = channel.socket().getRemoteSocketAddress().toString();
            if (sharedChannels.containsKey(name)) {
                List<String> keys = new ArrayList<String>(sharedChannels.keySet());
                int index = 0;
                for (String key : keys) {
                    if (name.equals(key)) {
                        break;
                    }
                    index++;
                }

                // put empty file list
                sharedIndexed.put(index, new ArrayList<String>());
                String indexName = index + ".index";
                FileUtils.writeFile(SERVER_FOLDER + indexName, new String("").getBytes(), false);
                System.out.println("Create file index: " + indexName);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * handler shared clinet file index
     * 
     * @param channel
     */
    private void handlerSharedSync(SocketChannel channel) {
        try {
            String name = channel.socket().getRemoteSocketAddress().toString();
            if (sharedChannels.containsKey(name)) {
                List<String> keys = new ArrayList<String>(sharedChannels.keySet());
                int index = 0;
                for (String key : keys) {
                    if (name.equals(key)) {
                        break;
                    }
                    index++;
                }
                String indexName = index + ".index";
                String length = ChannelUtils.readTop100(channel);
                byte[] contents = ChannelUtils.readFile(channel, Integer.parseInt(length));
                FileUtils.writeFile(SERVER_FOLDER + indexName, contents, false);
                System.out.println("Create file index: " + indexName);
                // load file index
                initFileText();
            }
        } catch (Exception e) {
            e.printStackTrace();
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
                    c.close();
                    downloadChannels.remove(fileName);
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
    private void send(File file, SocketChannel channel) throws Exception {
        String fileName = file.getFileName();
        byte[] fileNameBytes = StringUtils.fullSpace(fileName).getBytes();
        byte[] contents = file.getContents();
        byte[] lengthBytes = StringUtils.fullSpace(file.getContents().length).getBytes();
        byte[] array = new byte[fileNameBytes.length + lengthBytes.length + contents.length];

        ArrayUtils.arrayBytesCopy(fileNameBytes, array, 0);
        ArrayUtils.arrayBytesCopy(lengthBytes, array, fileNameBytes.length);
        ArrayUtils.arrayBytesCopy(contents, array, fileNameBytes.length + lengthBytes.length);
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
     * TODO need override alive channels to used NOTICE: MUST wait the result receivce
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
                            if (replis != null && !replis.isEmpty()) {
                                for (SocketChannel s : replis) {
                                    String currentRepliKey = s.socket().getRemoteSocketAddress().toString();
                                    if (badChannels.containsKey(currentRepliKey)) {

                                        // judge if the this replication was a bad channel but the bad count < 2 is
                                        // valid
                                        if (badChannels.get(currentRepliKey) < 2) {

                                            insteadShared(keys, i, key, s, currentRepliKey);
                                            break;
                                        }
                                    } else {

                                        // this replication is avaliable
                                        // replace the shared
                                        insteadShared(keys, i, key, s, currentRepliKey);
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

            // get shared file index
            syncSharedFileIndex();
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
     * replication instead shared
     * 
     * @param keys
     * @param i
     * @param key
     * @param s
     * @param currentRepliKey
     */
    private void insteadShared(List<String> keys, int i, String key, SocketChannel s, String currentRepliKey) {
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

    /**
     * sync shared file index
     */
    private void syncSharedFileIndex() {
        try {
            if (!sharedChannels.isEmpty()) {

                List<String> keys = new ArrayList<String>();
                // if file index list < current shared channels size, maybe some channels no return
                if (sharedIndexed.size() < sharedChannels.size()) {
                    List<Integer> ids = new ArrayList<Integer>(sharedIndexed.keySet());
                    if (ids.size() == 0) {
                        keys = new ArrayList<String>(sharedChannels.keySet());
                    } else {

                        List<String> sharedChannelKeys = new ArrayList<String>(sharedChannels.keySet());
                        // according to shared index list sequece to find channels which need to get file index
                        int max = sharedChannels.size();
                        for (int i = 0; i < max; i++) {
                            if (!ids.contains(i)) {
                                keys.add(sharedChannelKeys.get(i));
                            }
                        }
                    }
                }

                for (String key : keys) {
                    SocketChannel channel = sharedChannels.get(key);
                    System.out.println("Server sync shared file index: " + channel.socket().getRemoteSocketAddress());
                    send(Code.SERVER_SYNC_SHARED_INDEX_FILE, channel);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * start replica sync
     * 
     * @throws Exception
     */
    protected void collectReplicaVersion() throws Exception {
        if (!replicationChannels.isEmpty()) {
            List<String> keys = new ArrayList<String>(replicationChannels.keySet());
            for (String key : keys) {
                List<SocketChannel> channels = replicationChannels.get(key);
                for (SocketChannel channel : channels) {
                    byte[] returnCode = StringUtils.fullSpace(Code.SERVER_SEND_REPLICATION_TO_SYNC_SHARED.getCode()).getBytes();
                    ChannelUtils.write(channel, returnCode);
                }
            }
        }
    }

    /**
     * start replica sync
     */
    protected void syncReplica() throws Exception {
        if (!sharedChannels.isEmpty()) {
            List<String> keys = new ArrayList<String>(sharedChannels.keySet());
            int index = 0;
            for (String key : keys) {
                if (replicationChannels.containsKey(key)) {
                    String fileName = index + Constants.REPLICATION_VERSION_POST;
                    String smallVersion = ""; 

                    // read the replica version file and write current replica version
                    Map<String, String[]> readLine = FileUtils.readFileListLine(SERVER_FOLDER + fileName, " ");
                    StringBuilder builder = new StringBuilder();
                    List<String> mapKeys = new ArrayList<String>(readLine.keySet());
                    for (String mapKey : mapKeys) {
                        String[] array = readLine.get(mapKey);
                        builder.append(array[0] + " " + array[1] + " " + Constants.REPLICATION_RUNNING + "\r\n");

                        // if this channel is not sync, So not to need find the small version, Becaues this is small
                        smallVersion = array[1];
                        if (!smallVersion.equals(Constants.NO_VERSION)) {

                            // judge the small version
                            if (array[1].equals(Constants.NO_VERSION)) {
                                smallVersion = Constants.NO_VERSION;
                            } else {
                                String str = array[1].replace("V.", "");
                                try {
                                    int channelVer = Integer.parseInt(str);
                                    int smallVersinInt = Integer.parseInt(smallVersion.replace("V.", ""));
                                    if (smallVersinInt > channelVer) {
                                        smallVersion = String.valueOf(channelVer);
                                    }
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    System.err.println("The replica version is not valid ?");
                                }
                            }
                        } else {
                            
                            // the channels has No_version, So the smallest is No_ver
                            break;
                        }
                        System.out.println("The shared:" + key + " replicas smallest version is:" + smallVersion);
                    }
                    if(builder.length() > 0) {
                        
                        // Write file, update channels status is running.
                        FileUtils.writeFile(SERVER_FOLDER + fileName, builder.toString().getBytes(), false);
                    }
                    
                    // major channel
                    SocketChannel sharedChannel = sharedChannels.get(key);

                    // send the code and the version to shared
                    if (sharedChannel != null) {
                        String code = StringUtils.fullSpace(Code.REPLICATION_SYNC_SHARED.getCode());
                        String version = StringUtils.fullSpace(smallVersion);
                        ChannelUtils.write(sharedChannel, new String(code + version));
                    }
                    
                }
                index++;
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
