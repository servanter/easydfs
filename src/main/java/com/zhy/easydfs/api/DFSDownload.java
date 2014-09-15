package com.zhy.easydfs.api;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class DFSDownload {

    public static void main(String[] args) {
        String str = aaaa();
        System.out.println(str);
    }

    public static String aaaa() {
        SocketChannel channel = null;
        byte[] a = new byte[3];
        try {
            InetSocketAddress socketAddress = new InetSocketAddress("localhost", 10000);
            channel = SocketChannel.open(socketAddress);
            channel.configureBlocking(true);
            ByteBuffer buffer = ByteBuffer.allocate(3);// 创建1024字节的缓冲

            channel.write(ByteBuffer.wrap("bbb".getBytes()));
            while (channel.isOpen() && channel.read(buffer) >0) {
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
        return new String(a);
    }
}
