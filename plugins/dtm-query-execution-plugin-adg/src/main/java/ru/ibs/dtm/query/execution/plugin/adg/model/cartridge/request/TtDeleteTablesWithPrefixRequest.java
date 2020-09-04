package ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TtDeleteTablesWithPrefixRequest {
    private String tablePrefix;
}
