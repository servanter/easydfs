package com.zhy.easydfs.util;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    
    public static String readFileLine(String filePath) throws Exception {
        StringBuilder result = new StringBuilder();
        FileInputStream fis = new FileInputStream(filePath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
        String str = null;
        while ((str = reader.readLine()) != null) {
            result.append(str);
        }
        reader.close();
        fis.close();
        return result.toString();
    }
    
    public static List<String> readFileListLine(String filePath) throws Exception {
        List<String> result = new ArrayList<String>();
        FileInputStream fis = new FileInputStream(filePath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
        String str = null;
        while ((str = reader.readLine()) != null) {
            result.add(str);
        }
        reader.close();
        fis.close();
        return result;
    }

    public static Map<String, String[]> readFileListLine(String filePath, String split) throws Exception {
        Map<String, String[]> result = new LinkedHashMap<String, String[]>();
        FileInputStream fis = new FileInputStream(filePath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
        String str = null;
        while ((str = reader.readLine()) != null) {
            String[] arr = str.split(split);
            if (arr.length > 1) {
                result.put(arr[0], arr);
            }
        }
        reader.close();
        fis.close();
        return result;
    }
    
    public static String readVersion(String filePath) throws Exception{
        FileInputStream fis = new FileInputStream(filePath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
        String version = "";
        String str = null;
        while ((str = reader.readLine()) != null) {
            Pattern pattern = Pattern.compile("V.\\d{10}");
            Matcher matcher = pattern.matcher(str.trim());
            if(matcher.find()) {
                version = matcher.group();
            }
        }
        reader.close();
        fis.close();
        return version;
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
