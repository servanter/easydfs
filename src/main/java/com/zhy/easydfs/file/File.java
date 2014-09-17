package com.zhy.easydfs.file;

import java.util.Arrays;

public class File {

    private String fileName;

    private int contentLength;

    private byte[] contents;

    public File() {

    }

    public File(String fileName, int contentLength, byte[] contents) {
        this.fileName = fileName;
        this.contentLength = contentLength;
        this.contents = contents;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public int getContentLength() {
        return contentLength;
    }

    public void setContentLength(int contentLength) {
        this.contentLength = contentLength;
    }

    public byte[] getContents() {
        return contents;
    }

    public void setContents(byte[] contents) {
        this.contents = contents;
    }

    @Override
    public String toString() {
        return "File [fileName=" + fileName + ", contentLength=" + contentLength + ", contents=" + Arrays.toString(contents) + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fileName == null) ? 0 : fileName.hashCode());
        return Math.abs(result);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        File other = (File) obj;
        if (contentLength != other.contentLength)
            return false;
        if (!Arrays.equals(contents, other.contents))
            return false;
        if (fileName == null) {
            if (other.fileName != null)
                return false;
        } else if (!fileName.equals(other.fileName))
            return false;
        return true;
    }

}
