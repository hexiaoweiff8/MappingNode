package org.mappingnode.idmapping.entity;

import lombok.Data;

import javax.persistence.Column;
import javax.persistence.Table;
import java.io.Serializable;

/**
 * 映射表
 */
@Data
@Table(name = "id_mapping")
public class IdMapping implements Serializable {

    /**
     * fromId
     */
    @Column(name = "from_id")
    private String fromId;

    /**
     * toId
     */
    @Column(name = "to_id")
    private String toId;

}
