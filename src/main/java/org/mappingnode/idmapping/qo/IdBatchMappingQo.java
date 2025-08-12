package org.mappingnode.idmapping.qo;

import lombok.*;

import java.util.List;

/**
 * id映射Qo
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class IdBatchMappingQo {

    /**
     * 来源Id列表
     */
    private List<IdMappingQo> ids;
}
