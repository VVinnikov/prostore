package ru.ibs.dtm.liquibase.model;

import liquibase.Liquibase;
import lombok.*;

@Data
@NoArgsConstructor
@RequiredArgsConstructor
public class LiquibaseContext {
    @NonNull
    private LiquibaseCommand command;
    @NonNull
    private Liquibase liquibase;
}
