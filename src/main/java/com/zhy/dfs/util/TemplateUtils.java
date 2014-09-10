package com.zhy.dfs.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.text.MessageFormat;
import java.util.Properties;

/**
 * 
 * 
 * @author zhy
 * 
 */
public class TemplateUtils {

    /**
     * 
     */
    private static Properties properties;

    static {
        if (properties == null) {
            try {
                properties = new Properties();
                InputStream inputStream = TemplateUtils.class.getClassLoader().getResourceAsStream("template.properties");
                properties.load(new InputStreamReader(inputStream, "UTF-8"));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 返回模板信息
     * 
     * @param key
     * @param args
     * @return
     */
    public static String getMessage(String key, String... args) {
        String result = "";
        if (properties.containsKey(key)) {
            result = properties.getProperty(key);
        }
        if (args != null && args.length > 0) {
            return MessageFormat.format(result, args);
        }
        return result;
    }

    public static boolean contains(String key) {
        return properties.contains(key);
    }

}
