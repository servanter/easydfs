package com.zhy.easydfs.constants;

/**
 * transfer code
 * 
 * @author zhanghongyan
 * 
 */
public enum Code {

    /**
     * is not a code
     */
    NULL_SIGH_CODE(99999),

    /**
     * server accpect to client successfuly
     */
    SERVER_ACCPECT_SUCCESS(10001),

    /**
     * As the system channel
     */
    SYSTEM_CHANNEL(10101),

    /**
     * server monitor client
     */
    SERVER_HEARTBEAT(20001),

    /**
     * client return message
     */
    CLIENT_HEARTBEAT(20002),
    
    /**
     * server request shared index file
     */
    SERVER_SYNC_SHARED_INDEX_FILE(20011),
    
    /**
     * server request shared index file, the shared return success
     */
    SERVER_SYNC_SHARED_INDEX_FILE_SUCCESS(20012),
    
    /**
     * server request shared index file, the shared hasn't files
     */
    SERVER_SYNC_SHARED_INDEX_FILE_EMPTY(20013),
    
    /**
     * server send a request to every replication and collection current version
     */
    SERVER_SEND_REPLICATION_TO_SYNC_SHARED(20021),
    
    /**
     * replication request sync to shared
     */
    REPLICATION_SYNC_SHARED(20022),
    
    /**
     * shared receive replication requests
     */
    SHARED_RECEIVE_SYNC_AND_SEND_REPLICATION(20023),
    
    /**
     * shared send to repli
     */
    SERVER_RECEIVE_SHARED_AND_SEND_TO_REPLICATION(20024),
    
    /**
     * repli receive file success
     */
    REPLICATION_RECEIVE_SHARED_SUCCESS(20025),
    
    /**
     * the shared file is locked, Please try again for a moment
     */
    SHARED_VERSION_FILE_IS_LOCKED(20029),

    /**
     * request download
     */
    OPT_DOWNLOAD(30001),

    /**
     * request download success sign
     */
    OPT_DOWNLOAD_SUCCESS(30099),

    /**
     * server request client to find the file
     */
    OPT_DOWNLOAD_REQUEST_CLIENT(30011),

    /**
     * client find the file
     */
    OPT_DOWNLOAD_FOUND_FILE(30012),

    /**
     * could not find the file
     */
    OPT_DOWNLOAD_NOT_FOUND_FILE(30013),

    /**
     * request upload
     */
    OPT_UPLOAD(30101),

    /**
     * result success
     */
    OPT_UPLOAD_SUCCESS(30102);

    private int code;

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    private Code(int code) {
        this.code = code;
    }

    public static Code codeConvert(int code) {
        Code[] codes = Code.values();
        for (Code c : codes) {
            if (code == c.getCode()) {
                return c;
            }
        }
        return Code.NULL_SIGH_CODE;
    }

}
