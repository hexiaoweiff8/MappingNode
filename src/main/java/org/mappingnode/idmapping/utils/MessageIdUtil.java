package org.mappingnode.idmapping.utils;

import cn.hutool.core.util.RandomUtil;

import java.time.LocalDateTime;

/**
 * 消息Id生成工具
 */
public class MessageIdUtil {

    /**
     * 本机Ip
     */
    public static String IP = null;

    static {
        IP = IPUtil.getIPAddress();
    }


    /**
     * 生成消息Id
     * @param accountId 账户Id
     * @return
     */
    public static String genMessageId(String accountId){
        return genMessageId(LocalDateTime.now(), HashUtil.getHashIntVal(accountId, 100000));
    }

    /**
     * 生成消息Id
     * @param accountId 账户Id
     * @return
     */
    public static String genMessageId(LocalDateTime time, Integer accountId) {
        return DateUtils.localDateTimeFormat(time, DateUtils.PATTERN_YYYYMMDDHHMMSSSSS)
                + String.format("%03d", HashUtil.getHashIntVal(accountId, 100000))
                + String.format("%03d", HashUtil.getHashIntVal(IP, 1000))
                + RandomUtil.randomString(UUIDUtil.getUUid(), 13);
    }
}
