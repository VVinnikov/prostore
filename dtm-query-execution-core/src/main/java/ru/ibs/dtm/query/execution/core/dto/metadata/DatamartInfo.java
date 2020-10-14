package ru.ibs.dtm.query.execution.core.dto.metadata;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DatamartInfo {
    private Integer id;
    private String mnemonic;
    private List<DatamartEntity> entities;

    public DatamartInfo(String mnemonic) {
        this.mnemonic = mnemonic;
    }

    public DatamartInfo(Integer id, String mnemonic) {
        this.id = id;
        this.mnemonic = mnemonic;
    }
}
