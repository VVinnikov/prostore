package io.arenadata.dtm.liquibase.model;

import liquibase.Liquibase;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@Data
@NoArgsConstructor
@RequiredArgsConstructor
public class LiquibaseContext {
    @NonNull
    private LiquibaseCommand command;
    @NonNull
    private Liquibase liquibase;
}
