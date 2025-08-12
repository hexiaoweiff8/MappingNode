package org.mappingnode.idmapping.mapper;


import org.apache.ibatis.annotations.Param;
import org.mappingnode.idmapping.entity.IdMapping;
import org.mappingnode.idmapping.qo.IdMappingQo;
import tk.mybatis.mapper.common.Mapper;

import java.util.List;


public interface IdMappingMapper extends Mapper<IdMapping> {

    /**
     * 创建表
     */
    void createTable(@Param("tableName") String tableName);

    /**
     * 判断是否存在
     */
    boolean isTableExist(@Param("tableName") String tableName);

    /**
     * 查询fromId
     */
    String selectFromId(@Param("tableName") String tableName, @Param("toId") String toId);

    /**
     * 查询toId
     */
    String selectToId(@Param("tableName") String tableName, @Param("fromId") String fromId);

    /**
     * 插入数据, fromId包含时间戳, 按照大小判断直接插入
     * @param qoList 入库列表
     * @return
     */
    int insertTo(@Param("tableName") String tableName, @Param("list") List<IdMappingQo> qoList);

    /**
     * @param expiredId 超时Id
     * @return
     */
    int deleteExpired(@Param("tableName") String tableName, @Param("expiredId") String expiredId, @Param("limit") Integer limit);

    /**
     * 批量删除
     */
    int deleteBatchWithFromId(@Param("tableName") String tableName, List<String> ids);

    /**
     * 批量删除
     */
    int deleteBatchWithToId(@Param("tableName") String tableName, List<String> ids);

}
