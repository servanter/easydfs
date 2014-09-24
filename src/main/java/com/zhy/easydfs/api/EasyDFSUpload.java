package com.zhy.easydfs.api;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import com.zhy.easydfs.constants.Code;
import com.zhy.easydfs.util.ArrayUtils;
import com.zhy.easydfs.util.ChannelUtils;
import com.zhy.easydfs.util.FileUtils;
import com.zhy.easydfs.util.NumberUtils;
import com.zhy.easydfs.util.StringUtils;
import com.zhy.easydfs.util.TemplateUtils;

public class EasyDFSUpload {

    private static SocketChannel channel;

    public static void main(String[] args) {
        try {
            EasyDFSUpload dfsUpload = new EasyDFSUpload();
            boolean isSuccess = dfsUpload.upload("D:\\", "BaiduP2PService.exe");
            System.out.println("upload is " + isSuccess);
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
        String code = ChannelUtils.readTop100(channel);
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
        byte[] fileNameBytes = StringUtils.fullSpace(fileName).getBytes();
        byte[] byteArray = FileUtils.readFile(fileFolder + fileName);
        byte[] array = new byte[fileNameBytes.length + byteArray.length];
        ArrayUtils.arrayBytesCopy(fileNameBytes, array, 0);
        ArrayUtils.arrayBytesCopy(byteArray, array, fileNameBytes.length);
        channel.write(ByteBuffer.wrap(array));
        channel.socket().shutdownOutput();
    }
}
