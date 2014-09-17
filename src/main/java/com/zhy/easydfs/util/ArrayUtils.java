package com.zhy.easydfs.util;


/**
 * array utils
 * 
 * @author zhanghongyan
 *
 */
public class ArrayUtils {

    public static void arrayBytesCopy(byte[] args1, byte[] args2, int index) {
        System.arraycopy(args1, 0, args2, index, args1.length);
    }
    
}
