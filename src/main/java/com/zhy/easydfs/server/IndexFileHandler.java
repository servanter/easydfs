package com.zhy.easydfs.server;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class IndexFileHandler implements Runnable {

    private File file;

    private String text;

    public IndexFileHandler(File file, String text) {
        this.file = file;
        this.text = text;
    }

    @Override
    public void run() {
        synchronized (IndexFileHandler.class) {
            FileWriter fileWriter = null;
            try {
                fileWriter = new FileWriter(file, true);
                fileWriter.write(text + "\r\n");
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    fileWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
