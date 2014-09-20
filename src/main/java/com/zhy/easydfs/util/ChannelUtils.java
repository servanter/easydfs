package com.zhy.easydfs.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class ChannelUtils {

    /**
     * read first 100 bytes
     * 
     * @param channel
     * @return
     * @throws IOException
     */
    public static String readTop100(SocketChannel channel) throws IOException {
        int capacity = 100;
        byte[] fileBytes = new byte[100];
        String text = "";
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        if (channel.read(buffer) > 0) {
            buffer.flip();
            buffer.get(fileBytes);
            buffer.clear();
        }
        text = new String(fileBytes).trim();
        return text;
    }

    /**
     * read file from socket channel
     * 
     * @param channel
     * @param length
     * @return
     * @throws IOException
     */
    public static byte[] readFile(SocketChannel channel, Integer length) throws IOException {
        ByteBuffer dataBuffer = ByteBuffer.allocate(1024);
        int contentLength = 0;
        int size = -1;
        byte[] bytes = null;
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        while ((size = channel.read(dataBuffer)) >= 0) {
            contentLength += size;
            dataBuffer.flip();
            bytes = new byte[size];
            dataBuffer.get(bytes);
            byteArrayOutputStream.write(bytes);
            dataBuffer.clear();
            if (contentLength >= length) {
                break;
            }
        }

        byte[] byteArray = byteArrayOutputStream.toByteArray();
        byteArrayOutputStream.close();
        return byteArray;
    }

    /**
     * send str
     * 
     * @param channel
     * @param str
     * @throws Exception
     */
    public static void write(SocketChannel channel, String str) throws Exception {
        channel.write(ByteBuffer.wrap(str.getBytes()));
    }

    /**
     * send str
     * 
     * @param channel
     * @param contents
     * @throws Exception
     */
    public static void write(SocketChannel channel, byte[] contents) throws Exception {
        channel.write(ByteBuffer.wrap(contents));
    }
}
