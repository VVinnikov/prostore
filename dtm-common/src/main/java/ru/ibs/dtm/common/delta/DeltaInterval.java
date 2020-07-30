package ru.ibs.dtm.common.delta;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class DeltaInterval {
    private Long deltaFrom;
    private Long deltaTo;

    public String getIntervalStr(){
        return "(" + deltaFrom + "," + deltaTo + ")";
    }
}
