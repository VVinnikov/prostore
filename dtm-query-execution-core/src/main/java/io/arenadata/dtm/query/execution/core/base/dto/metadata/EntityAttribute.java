package io.arenadata.dtm.query.execution.core.base.dto.metadata;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EntityAttribute {
    @JsonIgnore
    private Integer id;
    private String mnemonic;
    private ColumnType dataType;
    private Integer length;
    private Integer accuracy;
    private String entityMnemonic;
    private String datamartMnemonic;
    private Integer primaryKeyOrder;
    private Integer distributeKeykOrder;
    private int ordinalPosition;
    private Boolean nullable;
}
