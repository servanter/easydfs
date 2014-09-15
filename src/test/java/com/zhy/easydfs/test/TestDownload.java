package com.zhy.easydfs.test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class TestDownload {

    public static void main(String[] args) {
        SocketChannel channel = null;
        try {
            InetSocketAddress socketAddress = new InetSocketAddress("localhost", 10000);
            channel = SocketChannel.open(socketAddress);
            channel.configureBlocking(true);
            ByteBuffer buffer = ByteBuffer.allocate(3);// 创建1024字节的缓冲
            byte[] a = new byte[3];

            while (channel.isOpen() && channel.read(buffer) != -1) {
                buffer.flip();
                buffer.get(a);
                System.out.println(new String(a));
                // 使用Charset.decode方法将字节转换为字符串
                buffer.clear();
            }
            System.out.println("12322222");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
