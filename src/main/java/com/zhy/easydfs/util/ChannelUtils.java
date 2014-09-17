package com.zhy.easydfs.util;

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
}
