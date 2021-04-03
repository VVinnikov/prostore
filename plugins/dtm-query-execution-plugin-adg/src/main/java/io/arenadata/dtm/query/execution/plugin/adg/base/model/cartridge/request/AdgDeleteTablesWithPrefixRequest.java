package io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AdgDeleteTablesWithPrefixRequest {
    private String tablePrefix;
}
