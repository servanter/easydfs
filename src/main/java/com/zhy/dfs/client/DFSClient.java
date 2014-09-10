package com.zhy.dfs.client;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

import com.zhy.dfs.file.File;
import com.zhy.dfs.util.TemplateUtils;

public class DFSClient {

    private static SocketChannel channel;

    private static Selector selector;
    
    private static String CLIENT_STORE_PATH;

    public static void main(String[] args) {
        try {
            if(args == null || args.length == 0) {
                System.exit(0);
            }
            CLIENT_STORE_PATH = args[0];
            DFSClient client = new DFSClient();
            DFSClientHandleThread clientHandleThread = new DFSClientHandleThread();
            clientHandleThread.init();
            new Thread(clientHandleThread).start();
            Thread.sleep(2000);
            client.send("D:\\", "aaa.txt", channel);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static final class DFSClientHandleThread implements Runnable {

        /**
         * init the client
         * 
         * @throws Exception
         */
        private void init() throws Exception {
            SocketChannel channel = SocketChannel.open();
            channel.configureBlocking(false);
            channel.connect(new InetSocketAddress(TemplateUtils.getMessage("server.host"), Integer.parseInt(TemplateUtils.getMessage("server.port"))));
            selector = Selector.open();
            channel.register(selector, SelectionKey.OP_CONNECT);
        }

        @Override
        public void run() {
            System.out.println("client start");
            while (true) {
                try {
                    selector.select();
                    Iterator<SelectionKey> it = selector.selectedKeys().iterator();
                    while (it.hasNext()) {
                        SelectionKey key = it.next();
                        it.remove();
                        if (key.isConnectable()) {

                            // connection the server
                            channel = (SocketChannel) key.channel();
                            if (channel.isConnectionPending()) {
                                channel.finishConnect();
                            }
                            channel.configureBlocking(false);
                            channel.register(selector, SelectionKey.OP_READ);
                        } else if (key.isReadable()) {
                            
                            // receive
                            File file = receiveData((SocketChannel) key.channel());
                            write(file);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        /**
         * persistence the data
         * 
         * @param file
         */
        private void write(File file) throws Exception {
            FileOutputStream fos = new FileOutputStream(new java.io.File(CLIENT_STORE_PATH + file.getFileName()));
            FileChannel fileChannel = fos.getChannel();
            ByteBuffer buffer = ByteBuffer.wrap(file.getContents());
            fileChannel.write(buffer);
            fileChannel.close();
//            fos.close();
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
//            channel.close();
            byteArrayOutputStream.close();
            return new File(fileName, contentLength, byteArrayOutputStream.toByteArray());
        }

    }

    /**
     * send file to shard
     * 
     * @param file
     */
    private void send(String fileFolder, String fileName, SocketChannel channel) throws Exception {
        System.out.println(2222222);
        StringBuilder builder = new StringBuilder();
        if (fileName.length() < 100) {
            for (int i = 0; i < 100 - fileName.length(); i++) {
                builder.append(" ");
            }
        }
        String fileAllName = fileName + builder.toString();
        byte[] fileNameBytes = fileAllName.getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        FileInputStream fis = new FileInputStream(new java.io.File(fileFolder + fileName));
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        FileChannel fileChannel = fis.getChannel();
        int size = 0;
        while ((size = fileChannel.read(buffer)) > 0) {
            buffer.flip();
            buffer.limit(size);
            byteArrayOutputStream.write(buffer.array());
//            channel.write(buffer);
            buffer.clear();
        }
        byte[] byteArray = byteArrayOutputStream.toByteArray();
        byte[] data = new byte[fileNameBytes.length + byteArray.length];
        for (int i = 0; i < fileNameBytes.length; i++) {
            data[i] = fileNameBytes[i];
        }
        int index = 0;
        for(int i = fileNameBytes.length; i < data.length; i++) {
            data[i] = byteArray[index];
            index++;
        }
        
        byteArrayOutputStream.close();
        channel.write(ByteBuffer.wrap(data));
    }

}