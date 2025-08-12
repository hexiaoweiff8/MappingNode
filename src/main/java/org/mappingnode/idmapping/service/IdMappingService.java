package org.mappingnode.idmapping.service;


import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.http.HttpUtil;
import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;
import lombok.extern.slf4j.Slf4j;
import org.mappingnode.idmapping.mapper.IdMappingMapper;
import org.mappingnode.idmapping.qo.IdMappingQo;
import org.mappingnode.idmapping.utils.HashUtil;
import org.mappingnode.idmapping.utils.MessageIdUtil;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.stereotype.Service;
import tk.mybatis.mapper.util.StringUtil;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.io.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Id映射Service
 */
@Slf4j
@Service
public class IdMappingService {

    /**
     * 线程池
     */
    private final ExecutorService executorService = new ThreadPoolExecutor(32,
            64,
            60,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(4096),
            new ThreadPoolExecutor.CallerRunsPolicy());


    /**
     * id映射Mapper
     */
    @Resource
    private IdMappingMapper idMappingMapper;

    /**
     * 服务发现实体类
     */
    @Resource
    private DiscoveryClient discoveryClient;

    /**
     * from本地缓存
     */
    private final ConcurrentHashMap<String, String> cacheFromMap = new ConcurrentHashMap<>(1280000);

    /**
     * to本地缓存
     */
    private final ConcurrentHashMap<String, String> cacheToMap = new ConcurrentHashMap<>(1280000);

    /**
     * 内存缓存超时小端堆
     * 用于处理超时内存缓存, 先出最早插入的id数据
     */
    private final PriorityBlockingQueue<String> memHeap = new PriorityBlockingQueue<>(1280000);

    /**
     * 内存缓存binLog小端堆
     */
    private final PriorityBlockingQueue<String> binLogHeap = new PriorityBlockingQueue<>(1280000);
    
    /**
     * 时间戳堆，维护<timestamp-id>格式的小端堆
     */
    private final PriorityBlockingQueue<String> timeHeap = new PriorityBlockingQueue<>(1280000);

    /**
     * 待入库mapList
     */
    private final ConcurrentHashMap<String, String> enterDbMap = new ConcurrentHashMap<>(540000);

    /**
     * 待入库mapList
     */
    private final ConcurrentHashMap<String, String> enterDbReMap = new ConcurrentHashMap<>(540000);

    /**
     * 待删除Map
     */
    private final ConcurrentHashMap<Long, List<String>> removeMap = new ConcurrentHashMap<>(540000);

    /**
     * 本地缓存删除队列 - fromId
     */
    private final ConcurrentHashMap<String, Set<String>> deleteFromIdMap = new ConcurrentHashMap<>();

    /**
     * 本地缓存删除队列 - toId
     */
    private final ConcurrentHashMap<String, Set<String>> deleteToIdMap = new ConcurrentHashMap<>();

    /**
     * 用于替代redisTemplateNumVal的本地缓存
     */
    private final ConcurrentHashMap<String, AtomicInteger> localCacheNumVal = new ConcurrentHashMap<>();

    /**
     * 服务实例列表
     */
    private List<ServiceInstance> instances;

    /**
     * hashMod值，默认64，
     */
    @Value("${idMapping.hashMod:64}")
    private int hashMod;

    /**
     * 入库线程数量
     */
    @Value("${idMapping.enterDbThreadSize:16}")
    private int enterDbThreadSize;

    /**
     * 极限数值, 超过该值出发入库清堆
     */
    @Value("${idMapping.limitCount:20000}")
    private int limitCount;

    /**
     * 单次删除数量
     */
    @Value("${idMapping.deleteCount:5000}")
    private int deleteCount;

    /**
     * 单次入库数量
     */
    @Value("${idMapping.batchCount:2000}")
    private int batchCount;

    /**
     * 映射库表名称
     */
    @Value("${idMapping.idMappingTableName:id_mapping_}")
    private String idMappingTableName;

    /**
     * 缓存域
     */
    @Value("${idMapping.cacheScope:idMapping}")
    private String cacheScope;

    /**
     * 内存缓存时间长度
     */
    @Value("${idMapping.memCacheTime:600}")
    private int memCacheTime;

    /**
     * db缓存时间长度
     */
    @Value("${idMapping.dbCacheTime:86400}")
    private int dbCacheTime;

    /**
     * 实例名称
     */
    @Value("${spring.application.name}")
    private String applicationName;

    /**
     * 删库key
     */
    private final String DELETE_DB_KEY = "delete_db";

    /**
     * 是否全部入库
     */
    private final AtomicBoolean allPush = new AtomicBoolean(false);

    /**
     * 入库游标
     */
    private volatile String enterDbCur = null;


    // --------------------------------------------公共方法------------------------------------------------

    /**
     * 初始化所有表
     */
    @PostConstruct
    public void initTable() {
        getServiceAInstances();
        for (int i = 0; i < hashMod; i++) {
            if (!idMappingMapper.isTableExist(idMappingTableName + "f" + i)) {
                idMappingMapper.createTable(idMappingTableName + "f" + i);
            }
            if (!idMappingMapper.isTableExist(idMappingTableName + "t" + i)) {
                idMappingMapper.createTable(idMappingTableName + "t" + i);
            }
        }
        // 替换redisTemplateNumVal为本地缓存
        localCacheNumVal.put(cacheScope, new AtomicInteger(0));
    }

    /**
     * 映射id
     *
     * @param fromId fromId
     * @param toId   toId
     */
    public void idMapping(String fromId, String toId) {
        // id放入缓存
        cacheFromMap.put(fromId, toId);
        cacheToMap.put(toId, fromId);
        memHeap.add(fromId);
        binLogHeap.add(fromId);
        // 添加时间戳-id到时间堆中
        long timestamp = System.currentTimeMillis();
        timeHeap.add(timestamp + "-" + fromId);
        if (cacheToMap.size() >= limitCount) {
            if (!allPush.get()) {
                allPush.set(true);
            }
        }
    }

    /**
     * 获取ToId
     * 如果redis不存在, 判断fromId的时间戳, 判断是否过期, 如果未过期则从mysql中获取
     *
     * @param fromId fromId
     * @return toId
     */
    public String getToId(String fromId) {
        String toId = getToIdFromMem(fromId);
        if (toId == null) {
            toId = getToIdFromInstances(fromId);
        }
        if (toId == null) {
            toId = idMappingMapper.selectToId(idMappingTableName + "f" + HashUtil.getHashIntVal(fromId, hashMod), fromId);
        }
        // 本地缓存获取, 如果没有葱redis获取, 如果还没有, 从db获取
        return toId;
    }

    /**
     * 获取FromId
     * 如果redis不存在, 从mysql中获取
     *
     * @param toId toId
     * @return fromId
     */
    public String getFromId(String toId) {
        String fromId = getFromIdFromMem(toId);
        if (fromId == null) {
            fromId = getFromIdFromInstances(toId);
        }
        if (fromId == null) {
            fromId = idMappingMapper.selectFromId(idMappingTableName + "t" + HashUtil.getHashIntVal(toId, hashMod), toId);
        }
        // 本地缓存获取, 如果没有葱redis获取, 如果还没有, 从db获取
        return fromId;
    }

    /**
     * 获取ToId
     * 如果redis不存在, 判断fromId的时间戳, 判断是否过期, 如果未过期则从mysql中获取
     *
     * @param fromId fromId
     * @return toId
     */
    public String getToIdFromMem(String fromId) {
        return cacheFromMap.getOrDefault(fromId, null);
    }

    /**
     * 获取FromId
     * 如果redis不存在, 从mysql中获取
     *
     * @param toId toId
     * @return fromId
     */
    public String getFromIdFromMem(String toId) {
        return cacheToMap.getOrDefault(toId, null);
    }

    /**
     * 删除Tokey
     *
     * @param id toId
     */
    public void deleteToId(String id) {
        // 删除本地缓存
        cacheToMap.remove(id);
        enterDbReMap.remove(id);
        // 删除数据库中的id
        String key = DELETE_DB_KEY + "t" + (System.currentTimeMillis() / 1000 % 2);
        deleteToIdMap.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet()).add(id);
    }

    /**
     * 删除Fromkey
     *
     * @param id fromId
     */
    public void deleteFromId(String id) {
        // 删除本地缓存
        cacheFromMap.remove(id);
        enterDbMap.remove(id);
        // 删除数据库中的id
        String key = DELETE_DB_KEY + "f" + (System.currentTimeMillis() / 1000 % 2);
        deleteFromIdMap.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet()).add(id);
    }

    /**
     * 堆入库
     */
    public void enterHeapDb() {
        log.info("堆入库 开始 heap: {}, form: {}, to: {}, enterDbMap: {}, enterDbReMap: {}, isAll: {}", memHeap.size(), cacheFromMap.size(), cacheToMap.size(), enterDbMap.size(), enterDbReMap.size(), allPush.get());
        allPush.set(false);
        try {
            if (timeHeap.size() > 0) {
                long currentTime = System.currentTimeMillis();
                long expireTime = currentTime - (allPush.get() ? 0 : memCacheTime * 1000L);
                
                while (timeHeap.size() > 0) {
                    String timeId = timeHeap.peek();
                    if (timeId == null) break;
                    
                    // 解析时间戳
                    int separatorIndex = timeId.indexOf('-');
                    if (separatorIndex <= 0) {
                        timeHeap.poll(); // 格式错误，移除
                        continue;
                    }
                    
                    long timestamp;
                    try {
                        timestamp = Long.parseLong(timeId.substring(0, separatorIndex));
                    } catch (NumberFormatException e) {
                        timeHeap.poll(); // 格式错误，移除
                        continue;
                    }
                    
                    // 如果时间未过期则停止处理
                    if (timestamp > expireTime) {
                        break;
                    }
                    
                    String fromId = timeId.substring(separatorIndex + 1);
                    String toId = cacheFromMap.get(fromId);
                    timeHeap.poll();
                    
                    if (toId != null) {
                        enterDbMap.put(fromId, toId);
                        enterDbReMap.put(toId, fromId);
                    } else {
                        log.info("错误映射: {}", fromId);
                    }
                }
            }
        } catch (Exception e) {
            log.error("error", e);
            this.getSize();
        }
        log.info("堆入库 结束 heap: {}, removeMap: {} form: {}, to: {}", timeHeap.size(), removeMap.size(), cacheFromMap.size(), cacheToMap.size());
    }

    /**
     * 处理缓存出入库
     */
    public void enterAndDeleteDb() {
        // 获取索引
        try {
            log.info("开始执行");
            // 替换redisTemplateNumVal为本地缓存
            AtomicInteger tmpIndexAtomic = localCacheNumVal.getOrDefault(cacheScope, new AtomicInteger(0));
            int tmpIndex = tmpIndexAtomic.getAndIncrement();
            localCacheNumVal.put(cacheScope, tmpIndexAtomic);
            log.info("处理入库");
            // 处理入库
            enterDb();
            log.info("处理主动删除");
            // 处理删除
            delDb(tmpIndex % 2);
            log.info("处理超时删除");
            delExpireDb();
            log.info("执行结束");
        } catch (Exception e) {
            log.error("定时处理出入库错误", e);
        }
    }

    /**
     * 打印大小
     */
    public void getSize() {
        log.info("from size: {}", ObjectSizeCalculator.getObjectSize(cacheFromMap));
        log.info("to size: {}", ObjectSizeCalculator.getObjectSize(cacheToMap));
        log.info("heap size: {}", ObjectSizeCalculator.getObjectSize(memHeap));
    }

    /**
     * 调用其他服务的内存id查询
     */
    public void getServiceAInstances() {
        List<ServiceInstance> instances = discoveryClient.getInstances(applicationName);
        this.instances = instances;
    }

    /**
     * 生成binlog文件
     */
    public void genBinLog(String path) {
        if (StringUtil.isEmpty(path)) {
            // 获取JAR文件所在目录
            path = IdMappingService.class.getProtectionDomain().getCodeSource().getLocation().getPath();
            path = new File(path).getParent();
        }

        try {
            BufferedWriter writer = null;
            List<String> batch = new ArrayList<>(1000);
            while (!binLogHeap.isEmpty()) {
                for (int i = 0; i < 1000 && !binLogHeap.isEmpty(); i++) {
                    String fromId = binLogHeap.poll();
                    if (fromId != null) {
                        batch.add(fromId);
                    }
                }

                if (batch.isEmpty()) {
                    continue;
                }

                // 确保当前文件写入器有效
                synchronized (this) {
                    String filePath = path + File.separator + "binlog_" + System.currentTimeMillis() + "_" + batch.get(0) + "_" + batch.get(batch.size() - 1) + "_" + batch.size() + ".log";
                    // 如果路径不存在创建路径
                    File file = new File(filePath);
                    if (!file.getParentFile().exists()) {
                        file.getParentFile().mkdirs();
                    }
                    writer = new BufferedWriter(new FileWriter(filePath, true), 8192);

                    // 写入日志文件，格式为 fromId,toId
                    for (String fromId : batch) {
                        String toId = cacheFromMap.get(fromId);
                        if (toId == null) {
                            log.warn("fromId: {} 的映射值为空，跳过写入binlog", fromId);
                            continue;
                        }
                        writer.write(fromId + "," + toId);
                        writer.newLine();
                    }
                    writer.flush();
                    writer.close();
                    writer = null;
                }
                batch.clear();
            }

            // 关闭最后一个文件写入器
            synchronized (this) {
                if (writer != null) {
                    writer.close();
                }
                if (enterDbCur != null) {
                    // 创建文件, 将游标写入文件
                    File cursorFile = new File(path + File.separator + "binlog_cursor.txt");
                    if (!cursorFile.exists()) {
                        cursorFile.createNewFile();
                    }
                    try (BufferedWriter curWriter = new BufferedWriter(new FileWriter(cursorFile))) {
                        curWriter.write(enterDbCur);
                        curWriter.flush();
                    }
                }
                if (enterDbCur != null) {
                    // 删除小于游标的文件
                    File[] files = new File(path).listFiles((dir, name) -> name.startsWith("binlog_") && name.endsWith(".log"));
                    for (File file : files) {
                        String fileName = file.getName();
                        String[] parts = fileName.split("_");
                        if (parts.length >= 4) {
                            String endFromId = parts[3];
                            if (endFromId.compareTo(enterDbCur) < 0) {
                                file.delete();
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("生成binlog文件时发生错误", e);
        }
    }

    /**
     * 加载Binlog文件到内存中
     *
     * @param path Binlog文件所在目录路径
     */
    public void loadBinLog(String path) {
        if (StringUtil.isEmpty(path)) {
            // 获取JAR文件所在目录
            path = IdMappingService.class.getProtectionDomain().getCodeSource().getLocation().getPath();
            path = new File(path).getParent();
        }
        File directory = new File(path);
        if (!directory.exists() || !directory.isDirectory()) {
            log.error("指定的路径不是一个有效的目录: {}", path);
            return;
        }

        File[] binLogFiles = directory.listFiles((dir, name) -> name.startsWith("binlog_") && name.endsWith(".log"));
        if (binLogFiles == null || binLogFiles.length == 0) {
            log.warn("目录中没有找到Binlog文件: {}", path);
            return;
        }

        // 读取游标
        File cursorFile = new File(path + File.separator + "binlog_cursor.txt");
        if (cursorFile.exists()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(cursorFile))) {
                String cursor = reader.readLine();
                if (cursor != null) {
                    enterDbCur = cursor;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        for (File binLogFile : binLogFiles) {
            try (BufferedReader reader = new BufferedReader(new FileReader(binLogFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(",");
                    if (parts.length == 2) {
                        String fromId = parts[0];
                        String toId = parts[1];
                        // 对比游标,如果游标大于当前fromId,则跳过该条记录
                        if (enterDbCur != null && fromId.compareTo(enterDbCur) <= 0) {
                            continue;
                        }
                        idMapping(fromId, toId);
                    } else {
                        log.warn("Binlog文件行格式错误: {}", line);
                    }
                }
            } catch (IOException e) {
                log.error("读取Binlog文件时发生错误: {}", binLogFile.getAbsolutePath(), e);
            } finally {
                // 加载完成后删除文件
                if (!binLogFile.delete()) {
                    log.warn("无法删除Binlog文件: {}", binLogFile.getAbsolutePath());
                }
            }
        }
        log.info("成功加载所有Binlog文件到内存中");
    }

    // ------------------------------------私有方法-------------------------------------

    /**
     * 从其他实例查询
     *
     * @param fromId
     */
    private String getToIdFromInstances(String fromId) {
        String toId = null;
        String ret = null;
        for (ServiceInstance instance : instances) {
            // 判断不是本机
//            if (instance.getHost().equals(IPUtil.getIPAddress())) {
//                continue;
//            }
            String url = "http://" + instance.getHost() + ":" + instance.getPort() + "/idMapping/getToIdFromMem?fromId=" + fromId; // 构造URL
            toId = HttpUtil.post(url, "");
            if (StringUtil.isNotEmpty(toId) && toId.length() < 40) {
                ret = toId;
                break;
            }
        }
        return ret;
    }

    /**
     * 从其他实例查询
     *
     * @param toId
     */
    private String getFromIdFromInstances(String toId) {
        String fromId = null;
        String ret = null;
        for (ServiceInstance instance : instances) {
            // 判断不是本机
//            if (instance.getHost().equals(IPUtil.getIPAddress())) {
//                continue;
//            }
            String url = "http://" + instance.getHost() + ":" + instance.getPort() + "/idMapping/getFromIdFromMem?toId=" + toId; // 构造URL
            fromId = HttpUtil.post(url, "");
            if (StringUtil.isNotEmpty(fromId) && fromId.length() < 40) {
                ret = fromId;
                break;
            }
        }
        return ret;
    }

    /**
     * 入库
     */
    private void enterDb() {
        log.info("enterDb start: {}", enterDbMap.size());
        if (enterDbMap.size() > 0) {
            HashMap<Integer, List<IdMappingQo>> listToHashMap = new HashMap<>();
            HashMap<Integer, List<IdMappingQo>> listFromHashMap = new HashMap<>();
            for (Map.Entry<String, String> entry : enterDbMap.entrySet()) {
                String fromId = entry.getKey();
                String toId = entry.getValue();
                Integer toHashVal = HashUtil.getHashIntVal(toId, hashMod);
                Integer fromHashVal = HashUtil.getHashIntVal(fromId, hashMod);
                List<IdMappingQo> enterToList = listToHashMap.getOrDefault(toHashVal, new ArrayList<>());
                List<IdMappingQo> enterFromList = listFromHashMap.getOrDefault(fromHashVal, new ArrayList<>());
                enterToList.add(IdMappingQo.builder()
                        .fromId(fromId)
                        .toId(toId)
                        .build());
                enterFromList.add(enterToList.get(enterToList.size() - 1));
                listToHashMap.put(toHashVal, enterToList);
                listFromHashMap.put(fromHashVal, enterFromList);
            }
            enterDbStage(listToHashMap, listFromHashMap);
            Long now = System.currentTimeMillis();
            List<String> beRemoveList = new ArrayList<>(enterDbMap.size());
            removeMap.put(now + memCacheTime * 1000L, beRemoveList);
            log.info("加入超时删除缓存: {}", now);
            for (Integer hash : listToHashMap.keySet()) {
                for (IdMappingQo idMappingQo : listToHashMap.get(hash)) {
                    enterDbMap.remove(idMappingQo.getFromId());
                    enterDbReMap.remove(idMappingQo.getToId());
                    // 放入待删除缓存
                    beRemoveList.add(idMappingQo.getFromId());
                }
            }
            listToHashMap.clear();
            listFromHashMap.clear();
        }
        // 清理超时缓存
        log.info("清理超时删除缓存 size: {}", removeMap.size());
        if (removeMap.size() > 0) {
            Long now = System.currentTimeMillis();
            Iterator<Map.Entry<Long, List<String>>> iterator = removeMap.entrySet().iterator();
            String toId = null;
            while (iterator.hasNext()) {
                Map.Entry<Long, List<String>> entry = iterator.next();
                Long time = entry.getKey();
                if (time - now < 0) {
                    List<String> ids = entry.getValue();
                    for (String fromId : ids) {
                        enterDbCur = fromId;
                        toId = cacheFromMap.remove(fromId);
                        if (StringUtil.isNotEmpty(toId)) {
                            cacheToMap.remove(toId);
                        } else {
                            log.info("fromId: {} 的映射值为空", fromId);
                        }
                    }
                    // 安全地移除过期的条目
                    iterator.remove();
                }
            }
        }
        log.info("enterDb end");
    }

    /**
     * 入库
     *
     * @param listToHashMap
     * @param listFromHashMap
     */
    private void enterDbStage(Map<Integer, List<IdMappingQo>> listToHashMap, Map<Integer, List<IdMappingQo>> listFromHashMap) {
        try {
            List<Integer> keyList = new ArrayList<>(listToHashMap.keySet());
            // 分线程
            int threadCount = Math.min(enterDbThreadSize, keyList.size());
            int preThreadCount = Math.max(1, keyList.size() / threadCount);
            CountDownLatch countDownLatch = new CountDownLatch(threadCount);
            for (int i = 0; i < threadCount; i++) {
                int start = i * preThreadCount;
                int end = Math.min((i + 1) * preThreadCount, keyList.size());
                log.info("i-start-end: {}-{}-{}", i, start, end);
                if (start > end) {
                    countDownLatch.countDown();
                    continue;
                }
                executorService.execute(() -> {
                    try {
                        List<Integer> subKeyList = keyList.subList(start, end);
                        for (Integer hashValTmp : subKeyList) {
                            List<IdMappingQo> enterListTmp = listToHashMap.get(hashValTmp);
                            if (CollectionUtil.isNotEmpty(enterListTmp)) {
                                // 分隔列表 10000一次
                                int loopCount = enterListTmp.size() / batchCount + (enterListTmp.size() % batchCount == 0 ? 0 : 1);
                                for (int j = 0; j < loopCount; j++) {
                                    List<IdMappingQo> tmpList = enterListTmp.subList(j * batchCount, Math.min((j + 1) * batchCount, enterListTmp.size()));
                                    try {
                                        idMappingMapper.insertTo(idMappingTableName + "t" + hashValTmp, tmpList);
                                    } catch (Exception ex) {
                                        log.error("enterDbStage..Exception2:", ex);
                                    }
                                    log.info("入库成功t{}: {}", hashValTmp, enterListTmp.size());
                                }
                            }
                        }
                        countDownLatch.countDown();
                    } catch (Exception ex) {
                        log.error("enterDbStage..Exception1:", ex);
                        countDownLatch.countDown();
                    }
                });
            }
            countDownLatch.await(600, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("toId入库失败", e);
        }
        try {
            List<Integer> keyList = new ArrayList<>(listFromHashMap.keySet());
            // 分线程
            int threadCount = Math.min(enterDbThreadSize, keyList.size());
            int preThreadCount = Math.max(1, keyList.size() / threadCount);
            CountDownLatch countDownLatch = new CountDownLatch(threadCount);
            for (int i = 0; i < threadCount; i++) {
                int start = i * preThreadCount;
                int end = Math.min((i + 1) * preThreadCount, keyList.size());
                log.info("i-start-end: {}-{}-{}", i, start, end);
                if (start > end) {
                    countDownLatch.countDown();
                    continue;
                }
                executorService.execute(() -> {
                    try {
                        List<Integer> subKeyList = keyList.subList(start, end);
                        for (Integer hashValTmp : subKeyList) {
                            List<IdMappingQo> enterListTmp = listFromHashMap.get(hashValTmp);
                            if (CollectionUtil.isNotEmpty(enterListTmp)) {
                                // 分隔列表 10000一次
                                int loopCount = enterListTmp.size() / batchCount + (enterListTmp.size() % batchCount == 0 ? 0 : 1);
                                for (int j = 0; j < loopCount; j++) {
                                    List<IdMappingQo> tmpList = enterListTmp.subList(j * batchCount, Math.min((j + 1) * batchCount, enterListTmp.size()));
                                    try {
                                        idMappingMapper.insertTo(idMappingTableName + "f" + hashValTmp, tmpList);
                                    } catch (Exception e) {
                                        log.error("toId入库失败", e);
                                    }
                                    log.info("入库成功f{}: {}", hashValTmp, enterListTmp.size());
                                }
                            }
                        }
                        countDownLatch.countDown();
                    } catch (Exception ex) {
                        log.error("enterDbStage..Exception:", ex);
                    }
                });
            }
            countDownLatch.await(600, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("fromId入库失败", e);
        }
    }

    /**
     * 单独入库
     */
    private void enterDbStage(Integer hashCode, String type, List<IdMappingQo> list) {
        Integer tmphash = 0;
        try {
            idMappingMapper.insertTo(idMappingTableName + type + hashCode, list);
            log.info("入库成功{}{}: {}", type, hashCode, list.size());
        } catch (Exception e) {
            log.error("toId入库失败", e);
        }
        list.clear();
    }

    /**
     * 删除超时数据库内容
     */
    private void delExpireDb() {
        try {
            // 处理删除
            String gapId = MessageIdUtil.genMessageId(LocalDateTime.now().minusSeconds(dbCacheTime), 0);
            for (int i = 0; i < hashMod; i++) {
                while (idMappingMapper.deleteExpired(idMappingTableName + "f" + i, gapId, deleteCount) > 0) ;
                while (idMappingMapper.deleteExpired(idMappingTableName + "t" + i, gapId, deleteCount) > 0) ;
            }
        } catch (Exception e) {
            log.error("delDb..Exception:", e);
        }
    }

    /**
     * 删除数据库内容
     *
     * @param index
     */
    private void delDb(int index) {
        // 处理删除
        Set<String> fromIds = deleteFromIdMap.get(DELETE_DB_KEY + "f" + index);
        Set<String> toIds = deleteToIdMap.get(DELETE_DB_KEY + "t" + index);
        
        HashMap<Integer, List<String>> idListHashMap = new HashMap<>();
        if (fromIds != null && fromIds.size() > 0) {
            try {
                // 组装map
                for (String fromId : fromIds) {
                    Integer fromHashVal = HashUtil.getHashIntVal(fromId, hashMod);
                    List<String> toList = idListHashMap.getOrDefault(fromHashVal, new ArrayList<>());
                    toList.add(fromId);
                    idListHashMap.put(fromHashVal, toList);
                }
                List<Integer> keyList = new ArrayList<>(idListHashMap.keySet());
                // 分线程
                int threadCount = Math.min(enterDbThreadSize, keyList.size());
                int preThreadCount = keyList.size() / threadCount;
                CountDownLatch countDownLatch = new CountDownLatch(threadCount);
                for (int i = 0; i < threadCount; i++) {
                    int start = i * preThreadCount;
                    int end = Math.min((i + 1) * preThreadCount, keyList.size());
                    executorService.execute(() -> {
                        try {
                            List<Integer> hashKeyList = keyList.subList(start, end);
                            for (Integer hashValTmp : hashKeyList) {
                                List<String> enterListTmp = idListHashMap.get(hashValTmp);
                                if (CollectionUtil.isNotEmpty(enterListTmp)) {
                                    // 分隔列表 10000一次
                                    int loopCount = enterListTmp.size() / batchCount + (enterListTmp.size() % batchCount == 0 ? 0 : 1);
                                    for (int j = 0; j < loopCount; j++) {
                                        List<String> tmpList = enterListTmp.subList(j * batchCount, Math.min((j + 1) * batchCount, enterListTmp.size()));
                                        delDbStage(tmpList, hashValTmp, "t");
                                        log.info("入库成功t{}: {} 开始删除内存数据2", hashValTmp, enterListTmp.size());
                                    }
                                }
                            }
                            countDownLatch.countDown();
                        } catch (Exception ex) {
                            log.error("delDb..Exception:", ex);
                        }
                    });
                }
                countDownLatch.await(600, TimeUnit.SECONDS);
                // 清空
                deleteFromIdMap.remove(DELETE_DB_KEY + "f" + index);
            } catch (Exception e) {
                log.error("删除失败", e);
            }
        }
        idListHashMap.clear();
        if (toIds != null && toIds.size() > 0) {
            try {
                // 组装map
                for (String toId : toIds) {
                    Integer toHashVal = HashUtil.getHashIntVal(toId, hashMod);
                    List<String> fromList = idListHashMap.getOrDefault(toHashVal, new ArrayList<>());
                    fromList.add(toId);
                    idListHashMap.put(toHashVal, fromList);
                }
                List<Integer> keyList = new ArrayList<>(idListHashMap.keySet());
                // 分线程
                int threadCount = Math.min(enterDbThreadSize, keyList.size());
                int preThreadCount = keyList.size() / threadCount;
                CountDownLatch countDownLatch = new CountDownLatch(threadCount);
                for (int i = 0; i < threadCount; i++) {
                    int start = i * preThreadCount;
                    int end = Math.min((i + 1) * preThreadCount, keyList.size());
                    executorService.execute(() -> {
                        try {
                            List<Integer> hashKeyList = keyList.subList(start, end);
                            for (Integer hashValTmp : hashKeyList) {
                                List<String> enterListTmp = idListHashMap.get(hashValTmp);
                                if (CollectionUtil.isNotEmpty(enterListTmp)) {
                                    // 分隔列表 10000一次
                                    int loopCount = enterListTmp.size() / batchCount + (enterListTmp.size() % batchCount == 0 ? 0 : 1);
                                    for (int j = 0; j < loopCount; j++) {
                                        List<String> tmpList = enterListTmp.subList(j * batchCount, Math.min((j + 1) * batchCount, enterListTmp.size()));
                                        delDbStage(tmpList, hashValTmp, "f");
                                        log.info("入库成功f{}: {} 开始删除内存数据2", hashValTmp, enterListTmp.size());
                                    }
                                }
                            }
                            countDownLatch.countDown();
                        } catch (Exception ex) {
                            log.error("delDb..Exception:", ex);
                        }
                    });
                }
                countDownLatch.await(600, TimeUnit.SECONDS);
                // 清空
                deleteToIdMap.remove(DELETE_DB_KEY + "t" + index);
            } catch (Exception e) {
                log.error("删除失败", e);
            }
            // 清空
            deleteToIdMap.remove(DELETE_DB_KEY + "t" + index);
        }
    }

    /**
     * 删除部分
     *
     * @param idListHashMap
     */
    private void delDbStage(HashMap<Integer, List<String>> idListHashMap, String tableSign) {
        for (Integer hashValTmp : idListHashMap.keySet()) {
            List<String> listTmp = idListHashMap.get(hashValTmp);
            if (CollectionUtil.isNotEmpty(listTmp)) {
                switch (tableSign) {
                    case "f":
                        idMappingMapper.deleteBatchWithFromId(idMappingTableName + tableSign + hashValTmp, listTmp);
                        break;
                    case "t":
                        idMappingMapper.deleteBatchWithToId(idMappingTableName + tableSign + hashValTmp, listTmp);
                        break;
                }
                listTmp.clear();
            }
        }
    }

    /**
     * 删除部分
     */
    private void delDbStage(List<String> idList, Integer hashVal, String tableSign) {
        if (CollectionUtil.isNotEmpty(idList)) {
            switch (tableSign) {
                case "f":
                    idMappingMapper.deleteBatchWithFromId(idMappingTableName + tableSign + hashVal, idList);
                    break;
                case "t":
                    idMappingMapper.deleteBatchWithToId(idMappingTableName + tableSign + hashVal, idList);
                    break;
            }
            idList.clear();
        }
    }
}