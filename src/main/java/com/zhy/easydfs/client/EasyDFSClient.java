package com.zhy.easydfs.client;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

import com.zhy.easydfs.constants.Code;
import com.zhy.easydfs.file.File;
import com.zhy.easydfs.util.ArrayUtils;
import com.zhy.easydfs.util.ChannelUtils;
import com.zhy.easydfs.util.FileUtils;
import com.zhy.easydfs.util.NumberUtils;
import com.zhy.easydfs.util.StringUtils;
import com.zhy.easydfs.util.TemplateUtils;

public class EasyDFSClient {

    private static Selector selector;

    private static String CLIENT_STORE_PATH;

    public static void main(String[] args) {
        try {
            if (args == null || args.length == 0) {
                System.exit(0);
            }
            CLIENT_STORE_PATH = args[0];
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
            default:
                break;
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
                return new File(fileName, Integer.parseInt(length), byteArray);
            }
            return null;
        }

    }

}
