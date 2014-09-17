package com.zhy.easydfs.api;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;

import com.zhy.easydfs.constants.Code;
import com.zhy.easydfs.file.File;
import com.zhy.easydfs.util.ArrayUtils;
import com.zhy.easydfs.util.ChannelUtils;
import com.zhy.easydfs.util.StringUtils;
import com.zhy.easydfs.util.TemplateUtils;

public class DFSDownload {

    public static void main(String[] args) {
        String str = download("listen.rar");
        System.out.println(str);
    }

    public static String download(String fileName) {
        SocketChannel channel = null;
        try {
            InetSocketAddress socketAddress = new InetSocketAddress(TemplateUtils.getMessage("server.host"), Integer.parseInt(TemplateUtils.getMessage("server.port")));
            channel = SocketChannel.open(socketAddress);
            channel.configureBlocking(true);

            byte[] codeBytes = StringUtils.fullSpace(Code.OPT_DOWNLOAD.getCode()).getBytes();
            byte[] fileBytes = StringUtils.fullSpace(fileName).getBytes();

            byte[] arr = new byte[codeBytes.length + fileBytes.length];
            ArrayUtils.arrayBytesCopy(codeBytes, arr, 0);
            ArrayUtils.arrayBytesCopy(fileBytes, arr, codeBytes.length);

            // send
            channel.write(ByteBuffer.wrap(arr));

            // receive
            String code = ChannelUtils.readTop100(channel);
            String name = ChannelUtils.readTop100(channel);
            String length = ChannelUtils.readTop100(channel);

            System.out.println("code =" + code + ", name=" + name + ", length=" + length);
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
                if (contentLength >= Integer.parseInt(length)) {
                    break;
                }
            }

            byte[] byteArray = byteArrayOutputStream.toByteArray();
            byteArrayOutputStream.close();

            FileOutputStream fos = new FileOutputStream(new java.io.File("D:\\nio\\download\\" + fileName));
            FileChannel fileChannel = fos.getChannel();
            ByteBuffer fileBuffer = ByteBuffer.wrap(byteArray);
            fileChannel.write(fileBuffer);
            fileChannel.close();
            fos.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }

}
