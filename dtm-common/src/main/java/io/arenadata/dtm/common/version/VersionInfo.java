package io.arenadata.dtm.common.version;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class VersionInfo {
    private String name;
    private String version;
}
