package org.mappingnode.idmapping.controller;

import lombok.extern.slf4j.Slf4j;
import org.mappingnode.idmapping.qo.IdBatchMappingQo;
import org.mappingnode.idmapping.qo.IdMappingQo;
import org.mappingnode.idmapping.service.IdMappingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(value = "/idMapping")
@Slf4j
public class IdMappingController {

    /**
     * id映射服务
     */
    @Autowired
    private IdMappingService idMappingService;


    /**
     * 映射Id
     * @param qo
     * @return
     */
    @PostMapping(value = "/mapping")
    public Boolean mapping(@RequestBody IdMappingQo qo) {
//        log.info("开始映射Id: {}", qo);
        idMappingService.idMapping(qo.getFromId(), qo.getToId());
        return true;
    }

    /**
     * 批量映射Id
     * @param qos
     * @return
     */
    @PostMapping(value = "/batchMapping")
    public Boolean batchMapping(IdBatchMappingQo qos) {
        log.info("开始批量映射Id: {}", qos.getIds().size());
        for (IdMappingQo qo : qos.getIds()) {
            idMappingService.idMapping(qo.getFromId(), qo.getToId());
        }
        return null;
    }

    /**
     * 获取toId
     * @return
     */
    @PostMapping(value = "/getToId")
    public String getToId(@RequestParam String fromId) {
        log.info("获取fromId: {}", fromId);
        return idMappingService.getToId(fromId);
    }

    /**
     * 获取fromId
     * @param toId
     * @return
     */
    @PostMapping(value = "/getFromId")
    public String getFromId(@RequestParam String toId) {
        log.info("获取toId: {}", toId);
        return idMappingService.getFromId(toId);
    }

    /**
     * 获取toId
     * @return
     */
    @PostMapping(value = "/getToIdFromMem")
    public String getToIdFromMem(@RequestParam String fromId) {
        return idMappingService.getToIdFromMem(fromId);
    }

    /**
     * 获取fromId
     * @param toId
     * @return
     */
    @PostMapping(value = "/getFromIdFromMem")
    public String getFromIdFromMem(@RequestParam String toId) {
        return idMappingService.getFromIdFromMem(toId);
    }

    /**
     * 删除toId
     * @return
     */
    @PostMapping(value = "/deleteToId")
    public void deleteToId(@RequestParam String toId) {
        idMappingService.deleteToId(toId);
    }

    /**
     * 删除fromId
     * @return
     */
    @PostMapping(value = "/deleteFromId")
    public void deleteFromId(@RequestParam String toId) {
        idMappingService.deleteFromId(toId);
    }

    /**
     * 输出内存占用
     * @return
     */
    @PostMapping(value = "/getMemSize")
    public void getMemSize() {
        idMappingService.getSize();
    }

}
