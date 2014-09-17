package com.zhy.easydfs.util;

/**
 * string utils
 * 
 * @author zhanghongyan
 * 
 */
public class StringUtils {

    /**
     * 
     * @param text
     * @return
     */
    public static String fullSpace(Object object) {
        String text = object.toString();
        StringBuilder builder = new StringBuilder();
        if (text.length() < 100) {
            for (int i = 0; i < 100 - text.length(); i++) {
                builder.append(" ");
            }
        }
        text += builder.toString();
        return text;
    }

    /**
     * merge string and get there bytes
     * 
     * @param args1
     * @param args2
     * @return
     */
    public static byte[] mergeAndGenerateBytes(String args1, String args2) {
        byte[] arr1 = args1.getBytes();
        byte[] arr2 = args2.getBytes();
        byte[] result = new byte[arr1.length + arr2.length];
        ArrayUtils.arrayBytesCopy(arr1, result, 0);
        ArrayUtils.arrayBytesCopy(arr2, result, arr1.length);
        return result;
    }
}
