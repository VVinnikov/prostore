package io.arenadata.dtm.query.execution.core.utils;

import io.arenadata.dtm.common.reader.InformationSchemaView;
import io.arenadata.dtm.query.execution.core.dto.metadata.InformationSchemaViewPosition;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Service for preparing query to information_schema
 */
public class MetaDataQueryPreparer {

    private static final Pattern FIND_TABLE = Pattern.compile(
            "\\s(from|join)\\s+([A-z.0-9\"]+)",
            Pattern.CASE_INSENSITIVE
    );

    /**
     * Find information_schema views in sql query
     *
     * @param sql query
     * @return List of information_schema positions
     */
    public static List<InformationSchemaViewPosition> findInformationSchemaViews(String sql) {
        List<InformationSchemaViewPosition> viewPositions = new ArrayList<>();
        Matcher matcher = FIND_TABLE.matcher(sql);
        while (matcher.find()) {
            InformationSchemaView view = InformationSchemaView.findByFullName(matcher.group(2));
            if (view != null) {
                viewPositions.add(
                        new InformationSchemaViewPosition(
                                view,
                                matcher.start(2),
                                matcher.end(2)));
            }
        }
        return viewPositions;
    }

    /**
     * Modify query
     *
     * @param sql query
     * @return modified query
     */
    public static String modify(String sql) {
        List<InformationSchemaViewPosition> viewPositions = findInformationSchemaViews(sql);
        if (CollectionUtils.isEmpty(viewPositions)) {
            return sql;
        }
        viewPositions.sort((vp1, vp2) -> Integer.compare(vp2.getStart(), vp1.getStart()));
        for (InformationSchemaViewPosition viewPosition : viewPositions) {
            sql = sql.substring(0, viewPosition.getStart()) +
                    viewPosition.getView().getRealName() + sql.substring(viewPosition.getEnd());
        }
        return sql;
    }
}
