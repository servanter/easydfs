package com.zhy.easydfs.server;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class FileOpt implements Runnable {

    private File file;

    private String text;

    public FileOpt(File file, String text) {
        this.file = file;
        this.text = text;
    }

    @Override
    public void run() {
        synchronized (FileOpt.class) {
            FileWriter fileWriter = null;
            try {
                fileWriter = new FileWriter(file, true);
                fileWriter.write(text);
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
