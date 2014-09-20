package com.zhy.easydfs.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
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

    /**
     * store file on the disk
     * 
     * @param filePath
     *            folder+filename
     * @param contents
     *            content
     * 
     * @param append
     * @return
     */
    public static boolean writeFile(String filePath, byte[] contents, boolean append) {
        try {
            FileWriter fileWriter = new FileWriter(new File(filePath), append);
            fileWriter.write(new String(contents));
            fileWriter.close();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * persistence the data
     * 
     * @param file
     */
    public static boolean write(com.zhy.easydfs.file.File file, String folder) {
        try {
            FileOutputStream fos = new FileOutputStream(new java.io.File(folder + file.getFileName()));
            FileChannel fileChannel = fos.getChannel();
            ByteBuffer buffer = ByteBuffer.wrap(file.getContents());
            fileChannel.write(buffer);
            fileChannel.close();
            fos.close();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
}
