package org.mappingnode.idmapping.utils;

/**
 * hash工具
 */
public class HashUtil {

    /**
     * 获取取余hash值
     * @param object
     * @param mod
     * @return
     */
    public static int getHashIntVal(Object object, int mod){
        return Math.abs(object.hashCode() % mod);
    }
}
