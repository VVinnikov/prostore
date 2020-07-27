package ru.ibs.dtm.query.execution.core.dto.metadata;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Data;
import ru.ibs.dtm.common.model.ddl.ColumnType;

@Data
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
