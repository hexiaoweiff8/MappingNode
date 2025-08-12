package org.mappingnode.idmapping.schedule;

import lombok.extern.slf4j.Slf4j;
import org.mappingnode.idmapping.service.IdMappingService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * id映射定时器
 */
@Slf4j
@Component
public class IdMappingSchedule {

    /**
     * id映射mq处理service
     */
    @Resource
    private IdMappingService idMappingService;

    /**
     * binlog路径
     */
    @Value("${binLogPath:}")
    private String binLogPath;

    /**
     * 本地锁 - 用于executeDb方法
     */
    private final ReentrantLock dbLock = new ReentrantLock();

    /**
     * 本地锁 - 用于executeDbCache方法
     */
    private final ReentrantLock dbCacheLock = new ReentrantLock();

    /**
     * 初始化所有表
     */
    @PostConstruct
    public void initTable() {
        // 启动定时删库进程
        ScheduledExecutorService executorDel = Executors.newScheduledThreadPool(1);
        executorDel.scheduleAtFixedRate(this::executeDbCache, 60, 10, TimeUnit.SECONDS);
        // 启动定时入库进程
        ScheduledExecutorService executorEnt = Executors.newScheduledThreadPool(1);
        executorEnt.scheduleAtFixedRate(this::executeDb, 60, 60, TimeUnit.SECONDS);
        // 启动定时获取服务实例进程
        ScheduledExecutorService executorGetIns = Executors.newScheduledThreadPool(1);
        executorGetIns.scheduleAtFixedRate(this::executeGetIns, 60, 60, TimeUnit.SECONDS);
        // 启动定时获取服务实例进程
        ScheduledExecutorService executorGenBinLog = Executors.newScheduledThreadPool(1);
        executorGenBinLog.scheduleAtFixedRate(this::executorGenBinLog, 60, 60, TimeUnit.SECONDS);

        idMappingService.loadBinLog(binLogPath);
    }

    /**
     * 执行一次出入库
     */
    public void executeDb() {
        log.info("id映射出入库");
        long st = System.currentTimeMillis();
        try {
            if (dbLock.tryLock(60, TimeUnit.SECONDS)) {
                try {
                    // 执行堆入库
                    idMappingService.enterAndDeleteDb();
                    // 获取所有服务实例
                    idMappingService.getServiceAInstances();
                } finally {
                    dbLock.unlock();
                    log.info("加锁解成功");
                }
            } else {
                log.info("加锁失败1");
            }
        } catch (Exception e) {
            log.error("id映射出入库执行异常：", e);
        }
        log.info("id映射出入库，耗时：{}ms", System.currentTimeMillis() - st);
    }

    /**
     * 执行一次入库
     */
    public void executeDbCache() {
        log.info("id映射出入入库缓存");
        long st = System.currentTimeMillis();
        try {
            if (dbCacheLock.tryLock(60, TimeUnit.SECONDS)) {
                // 执行堆入库
                idMappingService.enterHeapDb();
            } else {
                log.info("加锁失败2");
            }
        } catch (Exception e) {
            log.error("id映射出入入库缓存执行异常：", e);
        } finally {
            dbCacheLock.unlock();
            log.info("加锁解成功2");
        }
        log.info("id映射出入入库缓存，耗时：{}ms", System.currentTimeMillis() - st);
    }

    /**
     * 执行获取实例
     */
    public void executeGetIns() {
        log.info("获取服务实例列表");
        long st = System.currentTimeMillis();
        idMappingService.getServiceAInstances();
        log.info("获取服务实例列表，耗时：{}ms", System.currentTimeMillis() - st);
    }

    /**
     * 执行生成binlog
     */
    private void executorGenBinLog() {
        log.info("生成binlog");
        long st = System.currentTimeMillis();
        idMappingService.genBinLog(binLogPath);
        log.info("生成binlog，耗时：{}ms", System.currentTimeMillis() - st);
    }
}