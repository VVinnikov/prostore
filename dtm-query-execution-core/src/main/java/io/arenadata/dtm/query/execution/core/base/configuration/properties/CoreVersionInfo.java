package io.arenadata.dtm.query.execution.core.base.configuration.properties;

import io.arenadata.dtm.common.version.VersionInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.springframework.stereotype.Component;

@Data
@Component
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class CoreVersionInfo extends VersionInfo {
    public CoreVersionInfo() {
        super(CoreVersionInfo.class.getPackage().getSpecificationTitle(), CoreVersionInfo.class.getPackage().getImplementationVersion());
    }
}
