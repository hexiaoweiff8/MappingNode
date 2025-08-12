package org.mappingnode.idmapping.qo;

import lombok.*;

/**
 * id映射Qo
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class IdMappingQo {

    /**
     * 来源Id
     */
    private String fromId;

    /**
     * 去向Id
     */
    private String toId;

    /**
     * 目标表明
     */
    private String tableName;
}
