package com.zhy.dfs.util;

/**
 * number utils
 * 
 * @author zhanghongyan
 * 
 */
public class NumberUtils {

    /**
     * judge the str is nubmer
     * 
     * @param text
     * @return
     */
    public static boolean isInteger(String text) {
        try {
            Integer.parseInt(text);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
