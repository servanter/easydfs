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
    SERVER_ACCPECT_SUCCESS(10000),
    
    
    /**
     * As the system channel 
     */
    SYSTEM_CHANNEL(10100),

    /**
     * server monitor client
     */
    SERVER_HEARTBEAT(20000),
    
    /**
     * client return message
     */
    CLIENT_HEARTBEAT(20001),
    
    /**
     * request download
     */
    OPT_DOWNLOAD(30001),
    
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
