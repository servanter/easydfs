package com.zhy.easydfs.constants;

public enum Instructions {

    UNINSTRUCATIONS(""),

    ADD("ADD"),

    DEL("DEL"),

    UPD("UPD");

    private String desc;

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    private Instructions(String desc) {
        this.desc = desc;
    }

    public static Instructions code2Instructions(String str) {
        Instructions[] instructions = Instructions.values();
        for (Instructions instruction : instructions) {
            if (instruction.getDesc().equals(str)) {
                return instruction;
            }
        }
        return Instructions.UNINSTRUCATIONS;
    }

}
