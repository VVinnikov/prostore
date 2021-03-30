package io.arenadata.dtm.query.execution.core.dto.metadata;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import java.util.List;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DatamartEntity {
    @JsonIgnore
    private Long id;
    private String mnemonic;
    private String datamartMnemonic;
    private List<EntityAttribute> attributes;

    public DatamartEntity(Long id, String mnemonic, String datamartMnemonic) {
        this.id = id;
        this.mnemonic = mnemonic;
        this.datamartMnemonic = datamartMnemonic;
    }
}
