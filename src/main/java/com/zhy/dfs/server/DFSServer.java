package com.zhy.dfs.server;

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
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;

import com.zhy.dfs.file.File;
import com.zhy.dfs.util.TemplateUtils;

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
    public static final int TIME = 1000 * 60 * 5;

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
    private ConcurrentHashMap<String, List<SocketChannel>> replicationChannels = new ConcurrentHashMap<String, List<SocketChannel>>();

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
        if(args != null && args.length > 0) {
            SHAREDS = Integer.parseInt(args[0].replace("-shareds=", ""));
        }
        Timer timer = new Timer();
        timer.schedule(new DFSObserver(), TIME);
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
                keys.remove();
                if (key.isAcceptable()) {

                    // client request to accpect
                    ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
                    SocketChannel channel = serverSocketChannel.accept();
                    channel.configureBlocking(false);
                    channel.register(selector, SelectionKey.OP_READ);

                    // judge current connected channels is saturated
                    if (sharedChannels.size() < SHAREDS) {
                        sharedChannels.put("shard-" + sharedChannels.size(), channel);
                    } else {
                        List<String> sharedKeys = new ArrayList(sharedChannels.keySet());
                        if (replicationChannels.size() == 0) {
                            
                            // hasn't a replication
                            List<SocketChannel> reList = new ArrayList<SocketChannel>();
                            reList.add(channel);
                            replicationChannels.put(sharedKeys.get(0), reList);
                        } else {
                            boolean allHasRepli = true;
                            for(String sharedKey : sharedKeys) {
                                if(!replicationChannels.containsKey(sharedKey)) {
                                    
                                    // current shared has not replication
                                    allHasRepli = false;
                                    List<SocketChannel> reList = new ArrayList<SocketChannel>();
                                    reList.add(channel);
                                    replicationChannels.put(sharedKey, reList);
                                    break;
                                }
                            }
                            
                            // all shared has replication
                            if(allHasRepli) {
                                
                                // judge every replication size
                                String smallSharedKey = "";
                                int smallSize = MAX_SIZE;
                                for(String sharedKey : sharedKeys) {
                                    List<SocketChannel> chs = replicationChannels.get(sharedKey);
                                    if(chs != null) {
                                        if(chs.size() < smallSize) {
                                            smallSize = chs.size();
                                            smallSharedKey = sharedKey;
                                        }
                                    }
                                }
                                List<SocketChannel> chs = replicationChannels.get(smallSharedKey);
                                if(chs != null) {
                                    chs.add(channel);
                                }
                            }
                        }
                    }
                } else if (key.isReadable()) {
                    SocketChannel ch = (SocketChannel) key.channel();
                    File file = receiveData(ch);
                    write(file);

                    // shard and replication
                    int hashCode = file.hashCode();
                    int shard = hashCode % sharedChannels.size();
                    List<String> sharedKeys = new ArrayList(sharedChannels.keySet());
                    String currentKey = sharedKeys.get(shard);
                    SocketChannel channel = sharedChannels.get(currentKey);
                    channel.register(selector, SelectionKey.OP_READ);
                    
                    // send shared channel
                    send(file, channel);
                    
                    // send the shared channel replication
                    List<SocketChannel> replications = replicationChannels.get(currentKey);
                    for(SocketChannel socketChannel : replications) {
                        send(file, socketChannel);
                    }
                }
            }
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
    private File receiveData(SocketChannel channel) throws Exception {

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

}
