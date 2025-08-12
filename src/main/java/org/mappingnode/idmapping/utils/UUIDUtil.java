package org.mappingnode.idmapping.utils;

import java.util.UUID;

public class UUIDUtil {
	public static String  getUUid() {
		
		return UUID.randomUUID().toString().replace("-", "");
	}
}
