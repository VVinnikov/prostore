package ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("adqm.ddl")
public class DdlProperties {
    private String cluster;
    private Integer ttlSec;
    private String archiveDisk;

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public Integer getTtlSec() {
        return ttlSec;
    }

    public void setTtlSec(Integer ttlSec) {
        this.ttlSec = ttlSec;
    }

    public String getArchiveDisk() {
        return archiveDisk;
    }

    public void setArchiveDisk(String archiveDisk) {
        this.archiveDisk = archiveDisk;
    }
}
