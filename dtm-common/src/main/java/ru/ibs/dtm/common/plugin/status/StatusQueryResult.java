package ru.ibs.dtm.common.plugin.status;

import lombok.Data;

@Data
public class StatusQueryResult {
    private String topic;
    private String partition;
    private Long start;
    private Long end;
    private Long offset;
    private Long lag;
    private Long lastCommitTime;
}
