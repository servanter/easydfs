package com.zhy.easydfs.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileUtils {

    public static byte[] readFile(String filePath) throws Exception {
        return readFile(new File(filePath));
    }

    public static byte[] readFile(File file) throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        FileInputStream fis = new FileInputStream(file);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        FileChannel fileChannel = fis.getChannel();
        int size = 0;
        while ((size = fileChannel.read(buffer)) >= 0) {
            buffer.flip();
            buffer.limit(size);
            byteArrayOutputStream.write(buffer.array());
            buffer.clear();
        }
        byte[] byteArray = byteArrayOutputStream.toByteArray();
        byteArrayOutputStream.close();
        fileChannel.close();
        fis.close();
        return byteArray;
    }

}
