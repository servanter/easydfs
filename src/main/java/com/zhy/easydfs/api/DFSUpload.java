package com.zhy.easydfs.api;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;

import com.zhy.easydfs.constants.Code;
import com.zhy.easydfs.util.NumberUtils;
import com.zhy.easydfs.util.TemplateUtils;

public class DFSUpload {
    private static SocketChannel channel;

    public static void main(String[] args) {
        try {
            DFSUpload dfsUpload = new DFSUpload();
            dfsUpload.upload("D:\\", "car_info.js");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * init the client
     * 
     * @throws Exception
     */
    private boolean upload(String fileFolder, String fileName) throws Exception {
        InetSocketAddress socketAddress = new InetSocketAddress(TemplateUtils.getMessage("server.host"), Integer.parseInt(TemplateUtils.getMessage("server.port")));
        channel = SocketChannel.open(socketAddress);
        channel.configureBlocking(true);

        // send file
        send(fileFolder, fileName, channel);

        int capacity = 100;
        byte[] fileBytes = new byte[100];
        String code = "";
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        if (channel.isOpen() && channel.read(buffer) > 0) {
            buffer.flip();
            buffer.get(fileBytes);
            buffer.clear();
        }
        code = new String(fileBytes).trim();
        System.out.println("client receive code=" + code);
        if (code.length() > 0) {
            if (NumberUtils.isInteger(code)) {

                // is sign code
                Code sign = Code.codeConvert(Integer.parseInt(code));
                return (sign == Code.OPT_UPLOAD_SUCCESS);
            }
        }
        return false;
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
            buffer.clear();
        }
        byte[] byteArray = byteArrayOutputStream.toByteArray();
        byte[] data = new byte[fileNameBytes.length + byteArray.length];
        for (int i = 0; i < fileNameBytes.length; i++) {
            data[i] = fileNameBytes[i];
        }
        int index = 0;
        for (int i = fileNameBytes.length; i < data.length; i++) {
            data[i] = byteArray[index];
            index++;
        }

        channel.write(ByteBuffer.wrap(data));
        byteArrayOutputStream.close();
    }
}
