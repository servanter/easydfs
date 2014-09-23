package com.zhy.easydfs.client;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.zhy.easydfs.constants.Code;
import com.zhy.easydfs.constants.Constants;
import com.zhy.easydfs.constants.Instructions;
import com.zhy.easydfs.file.File;
import com.zhy.easydfs.server.FileOpt;
import com.zhy.easydfs.util.ArrayUtils;
import com.zhy.easydfs.util.ChannelUtils;
import com.zhy.easydfs.util.FileUtils;
import com.zhy.easydfs.util.NumberUtils;
import com.zhy.easydfs.util.StringUtils;
import com.zhy.easydfs.util.TemplateUtils;

public class EasyDFSClient {

    private static Selector selector;

    private static String CLIENT_STORE_PATH;

    private static String VERSION_FILE = "current.version";

    /**
     * transfer max file size
     */
    private static long MAX_FILE_SIZE = 1024 * 1024 * 50;

    public static void main(String[] args) {
        try {
            if (args == null || args.length == 0) {
                System.err.println("Please input client store folder.");
                System.exit(0);
            }
            CLIENT_STORE_PATH = args[0].replace("-folder=", "");
            DFSClientHandleThread clientHandleThread = new DFSClientHandleThread();
            clientHandleThread.init();
            new Thread(clientHandleThread).start();
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
                            SocketChannel channel = (SocketChannel) key.channel();
                            if (channel.isConnectionPending()) {
                                channel.finishConnect();
                            }
                            channel.configureBlocking(false);
                            codeHandler(Code.SYSTEM_CHANNEL, channel);
                            channel.register(selector, SelectionKey.OP_READ);
                        } else if (key.isReadable()) {
                            try {
                                dispatchHandler(key);
                            } catch (Exception e) {
                                key.cancel();
                                e.printStackTrace();
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
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
                if (code.length() > 0) {
                    System.out.println("Client receive the code: " + code);

                    if (NumberUtils.isInteger(code)) {

                        // is sign code
                        Code sign = Code.codeConvert(Integer.parseInt(code));
                        codeHandler(sign, channel);
                    } else {

                        // is file
                        File file = fileHandler(code, channel);
                        write(file);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw new Exception(e);
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

            // server monitor client
            case SERVER_HEARTBEAT:
                channel.write(ByteBuffer.wrap(StringUtils.fullSpace(Code.CLIENT_HEARTBEAT.getCode()).getBytes()));
                break;
            case SYSTEM_CHANNEL:
                channel.write(ByteBuffer.wrap(StringUtils.fullSpace(Code.SYSTEM_CHANNEL.getCode()).getBytes()));
                break;
            case OPT_DOWNLOAD_REQUEST_CLIENT:
                handlerDownload(channel);
                break;
            case SERVER_SYNC_SHARED_INDEX_FILE:
                handlerServerSync(channel);
                break;
            case SERVER_SEND_REPLICATION_TO_SYNC_SHARED:
                handlerServerCollectRepliVersion(channel);
                break;
            case REPLICATION_SYNC_SHARED:
                handlerReplicationSync(channel);
                break;
            case SERVER_RECEIVE_SHARED_AND_SEND_TO_REPLICATION:
                handlerStartReplicationSync(channel);
            default:
                break;
            }
        }

        /**
         * server collect repli version
         * 
         * @param channel
         */
        private void handlerServerCollectRepliVersion(SocketChannel channel) {
            try {
                String str = null;
                try {
                    str = FileUtils.readVersion(CLIENT_STORE_PATH + VERSION_FILE);
                } catch (Exception e) {
                    System.err.println("Hasn't no version file, because the channel will be began.");
                    str = Constants.NO_VERSION;
                }
                if(str == null || str.length() == 0) {
                    str = Constants.NO_VERSION;
                }
                byte[] returnCode = StringUtils.fullSpace(Code.REPLICATION_SYNC_SHARED.getCode()).getBytes();
                byte[] version = StringUtils.fullSpace(str).getBytes();
                byte[] array = new byte[returnCode.length + version.length];

                ArrayUtils.arrayBytesCopy(returnCode, array, 0);
                ArrayUtils.arrayBytesCopy(version, array, returnCode.length);
                ChannelUtils.write(channel, array);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        /**
         * receive the shared send message
         * 
         * @param channel
         */
        private void handlerStartReplicationSync(SocketChannel channel) {
            try {
                String version = ChannelUtils.readTop100(channel);
                String instruction = ChannelUtils.readTop100(channel);
                Instructions instructions = Instructions.code2Instructions(instruction);
                switch (instructions) {
                case ADD:
                    String fileName = ChannelUtils.readTop100(channel);
                    String fileLength = ChannelUtils.readTop100(channel);
                    System.out.println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA " + CLIENT_STORE_PATH + fileName);
                    byte[] content = ChannelUtils.readFile(channel, Integer.parseInt(fileLength));
                    FileUtils.writeFile(CLIENT_STORE_PATH + fileName, content, false);
                    System.out.println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA " + CLIENT_STORE_PATH + fileName);
                    
                    // write version
                    StringBuilder builder = new StringBuilder();
                    builder.append(version + "\r\n");
                    builder.append("A " + fileName + "\r\n");
                    FileUtils.writeFile(CLIENT_STORE_PATH + "current.version", builder.toString().getBytes(), true);

                    byte[] returnCode = StringUtils.fullSpace(Code.REPLICATION_RECEIVE_SHARED_SUCCESS.getCode()).getBytes();
                    byte[] afterVersion = StringUtils.fullSpace(version).getBytes();
                    byte[] array = new byte[returnCode.length + afterVersion.length];

                    ArrayUtils.arrayBytesCopy(returnCode, array, 0);
                    ArrayUtils.arrayBytesCopy(afterVersion, array, returnCode.length);
                    ChannelUtils.write(channel, array);
                    break;

                default:
                    break;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        /**
         * handler replication sync data
         * 
         * @param channel
         */
        private void handlerReplicationSync(SocketChannel channel) {
            try {
                String version = ChannelUtils.readTop100(channel);

                // find the current version in the file and read other str.
                List<String> fileLine = FileUtils.readFileListLine(CLIENT_STORE_PATH + "current.version");

                // the file format
                // V.123213123123
                // aaa.txt
                // bbb.txt
                // V.1232132222
                // ccc.txt
                // ddd.txt

                String afterReadingVersion = "";
                long fileSize = 0l;
                boolean hasVersion = false;       // this line is current version
                boolean next = false;             // next version which the replication looking for
                boolean canRead = false;
                for (String str : fileLine) {
                    if(version.equals(Constants.NO_VERSION)) {
                        canRead = true;
                    } else {
                        
                        // this line is this version 
                        if(!hasVersion) {
                            if (str.equals(version)) {
                                hasVersion = true;
                            }
                        }
                        
                        // judge next line is version content
                        if(!next) {
                            if(hasVersion) {
                                Pattern pattern = Pattern.compile("V.\\d{10}");
                                Matcher matcher = pattern.matcher(str.trim());
                                if (matcher.find()) {
                                    
                                    // This version is a current version of the transfer version
                                    afterReadingVersion = matcher.group();
                                    next = true;
                                }
                            }
                        }
                        canRead = hasVersion && next;
                    }
                    if(canRead){
                        Pattern pattern = Pattern.compile("V.\\d{10}");
                        Matcher matcher = pattern.matcher(str.trim());
                        if (matcher.find()) {

                            // already read this version
                            if (fileSize < MAX_FILE_SIZE) {
                                afterReadingVersion = matcher.group();
                            } else {
                                break;
                            }
                        } else if (str.split(" ")[0].equals("A")) {

                            // reserve 100 bytes
                            byte[] returnVersion = StringUtils.fullSpace(afterReadingVersion).getBytes();
                            byte[] instruction = StringUtils.fullSpace(Instructions.ADD).getBytes();
                            byte[] fileNameBytes = StringUtils.fullSpace(str.split(" ")[1]).getBytes();
                            byte[] fileContent = FileUtils.readFile(CLIENT_STORE_PATH + str.split(" ")[1]);
                            byte[] fileLength = StringUtils.fullSpace(fileContent.length).getBytes();
                            byte[] returnCode = StringUtils.fullSpace(Code.SHARED_RECEIVE_SYNC_AND_SEND_REPLICATION.getCode()).getBytes();
                            byte[] array = new byte[returnCode.length + returnVersion.length + instruction.length + fileNameBytes.length + fileLength.length + fileContent.length];

                            // sequence code, version, filename, length, content
                            ArrayUtils.arrayBytesCopy(returnCode, array, 0);
                            ArrayUtils.arrayBytesCopy(returnVersion, array, returnCode.length);
                            ArrayUtils.arrayBytesCopy(instruction, array, returnCode.length + returnVersion.length);
                            ArrayUtils.arrayBytesCopy(fileNameBytes, array, returnCode.length + returnVersion.length + instruction.length);
                            ArrayUtils.arrayBytesCopy(fileLength, array, returnCode.length + returnVersion.length + instruction.length + fileNameBytes.length);
                            ArrayUtils.arrayBytesCopy(fileContent, array, returnCode.length + returnVersion.length + instruction.length + fileNameBytes.length + fileLength.length);
                            fileSize += array.length;
                            ChannelUtils.write(channel, array);
                        } else if (false) {

                        }
                    }
                }

                // byte[] returnCode = StringUtils.fullSpace(Code.SHARED_RECEIVE_SYNC_AND_SEND_REPLICATION.getCode()).getBytes();
                // byte[] afterReadingVersionBytes = StringUtils.fullSpace(afterReadingVersion).getBytes();
                // replace version
                // for(byte[] b : list) {
                // ArrayUtils.arrayBytesCopy(afterReadingVersionBytes, b, returnCode.length);
                // ChannelUtils.write(channel, b);
                // }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        /**
         * server file index sync (load client file index)
         * 
         * @param channel
         */
        private void handlerServerSync(SocketChannel channel) throws Exception {
            java.io.File file = new java.io.File(CLIENT_STORE_PATH);
            java.io.File[] files = file.listFiles(new FilenameFilter() {

                @Override
                public boolean accept(java.io.File dir, String name) {
                    return !name.equals("current.version");
                }
            });
            if (files != null && files.length > 0) {
                StringBuilder builder = new StringBuilder();
                for (java.io.File f : files) {
                    builder.append(f.getName() + "\r\n");
                }
                byte[] returnCode = StringUtils.fullSpace(Code.SERVER_SYNC_SHARED_INDEX_FILE_SUCCESS.getCode()).getBytes();
                byte[] fileContent = builder.toString().getBytes();
                byte[] fileLength = StringUtils.fullSpace(fileContent.length).getBytes();
                byte[] array = new byte[returnCode.length + fileLength.length + fileContent.length];

                ArrayUtils.arrayBytesCopy(returnCode, array, 0);
                ArrayUtils.arrayBytesCopy(fileLength, array, returnCode.length);
                ArrayUtils.arrayBytesCopy(fileContent, array, returnCode.length + fileLength.length);
                ChannelUtils.write(channel, array);
            } else {

                // not store anyting, but must return code
                byte[] returnCode = StringUtils.fullSpace(Code.SERVER_SYNC_SHARED_INDEX_FILE_EMPTY.getCode()).getBytes();
                ChannelUtils.write(channel, returnCode);
            }

        }

        /**
         * control file download
         * 
         * @param channel
         */
        private void handlerDownload(SocketChannel channel) throws Exception {
            final String fileName = ChannelUtils.readTop100(channel);
            java.io.File file = new java.io.File(CLIENT_STORE_PATH);
            java.io.File[] fs = file.listFiles(new FilenameFilter() {

                @Override
                public boolean accept(java.io.File dir, String name) {
                    if (fileName.equals(name)) {
                        return true;
                    }
                    return false;
                }
            });
            if (fs != null && fs.length > 0) {
                java.io.File f = fs[0];
                byte[] returnCode = StringUtils.fullSpace(Code.OPT_DOWNLOAD_FOUND_FILE.getCode()).getBytes();
                byte[] fileNameBytes = StringUtils.fullSpace(fileName).getBytes();
                byte[] fileContent = FileUtils.readFile(f.getAbsoluteFile());
                byte[] fileLength = StringUtils.fullSpace(fileContent.length).getBytes();
                byte[] array = new byte[returnCode.length + fileNameBytes.length + fileLength.length + fileContent.length];

                // sequence code, filename, length, content
                ArrayUtils.arrayBytesCopy(returnCode, array, 0);
                ArrayUtils.arrayBytesCopy(fileNameBytes, array, returnCode.length);
                ArrayUtils.arrayBytesCopy(fileLength, array, returnCode.length + fileNameBytes.length);
                ArrayUtils.arrayBytesCopy(fileContent, array, returnCode.length + fileNameBytes.length + fileLength.length);
                channel.write(ByteBuffer.wrap(array));
                System.out.println("Download the file:" + fileName);
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
            fos.close();
        }

        /**
         * receive and handle the data
         * 
         * @param channel
         * @throws Exception
         */
        private File fileHandler(String fileName, SocketChannel channel) throws Exception {
            String length = ChannelUtils.readTop100(channel);
            if (NumberUtils.isInteger(length)) {
                byte[] byteArray = ChannelUtils.readFile(channel, Integer.parseInt(length));
                storeVersionFile(fileName, CLIENT_STORE_PATH);
                return new File(fileName, Integer.parseInt(length), byteArray);
            }
            return null;
        }

        /**
         * store server send version
         * 
         * @param version
         * @param path
         */
        private void storeVersionFile(String fileName, String path) {
            String versionFile = "current.version";
            String time = String.valueOf(System.currentTimeMillis()).substring(0, 10);
            StringBuilder builder = new StringBuilder();
            builder.append("V." + time + "\r\n");
            builder.append("A " + fileName + "\r\n");
            String text = builder.toString();
            new Thread(new FileOpt(new java.io.File(path + versionFile), text)).start();
        }

    }

}
