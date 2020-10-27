package io.arenadata.dtm.query.execution.core.dao.delta.zookeeper;

import io.arenadata.dtm.query.execution.core.dto.delta.DeltaWriteOp;
import io.arenadata.dtm.query.execution.core.dto.delta.DeltaWriteOpRequest;
import io.arenadata.dtm.query.execution.core.dto.delta.HotDelta;
import io.arenadata.dtm.query.execution.core.dto.delta.OkDelta;
import io.vertx.core.Future;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Dao for delta work
 */
public interface DeltaServiceDao {

    /**
     * State new delta_hot and get it number
     *
     * @param datamart - datamart name
     * @return - new delta hot number
     */
    Future<Long> writeNewDeltaHot(String datamart);

    /**
     * State new delta_hot and get it number
     *
     * @param datamart    - datamart name
     * @param deltaHotNum - delta hot number
     * @return - new delta hot number
     */
    Future<Long> writeNewDeltaHot(String datamart, Long deltaHotNum);

    /**
     * State delta_hot commit
     *
     * @param datamart - datamart name
     */
    Future<Long> writeDeltaHotSuccess(String datamart);

    /**
     * State delta_hot commit
     *
     * @param datamart     - datamart name
     * @param deltaHotDate - delta hot date
     */
    Future<Long> writeDeltaHotSuccess(String datamart, LocalDateTime deltaHotDate);

    /**
     * State the delta_hot rollback started
     *
     * @param datamart - datamart name
     */
    Future<Void> writeDeltaError(String datamart, Long deltaHotNum);

    /**
     * State the delta_hot rolled back
     *
     * @param datamart - datamart name
     */
    Future<Void> deleteDeltaHot(String datamart);

    /**
     * State new write operation and get it sys_cn number
     *
     * @param operation - delta operation
     * @return - sys_cn = (op num - 1) + deltaHot.cnFrom
     */
    Future<Long> writeNewOperation(DeltaWriteOpRequest operation);

    /**
     * State write operation success
     *
     * @param datamart - datamart name
     * @param synCn    - synCn
     */
    Future<Void> writeOperationSuccess(String datamart, long synCn);

    /**
     * State write operation error. Its a begin of erase procedure
     *
     * @param datamart - datamart name
     * @param synCn    - synCn
     */
    Future<Void> writeOperationError(String datamart, long synCn);

    /**
     * State write operation delete complete
     *
     * @param datamart - datamart name
     * @param synCn    - synCn
     */
    Future<Void> deleteWriteOperation(String datamart, long synCn);


    /**
     * Get the delta meta-data by number
     *
     * @param datamart - datamart name
     * @param num      - delta ok number
     * @return delta ok metadata
     */
    Future<OkDelta> getDeltaByNum(String datamart, long num);

    /**
     * Get the delta meta-data by dateTime
     *
     * @param datamart - datamart name
     * @param dateTime - dateTime
     * @return delta ok metadata
     */
    Future<OkDelta> getDeltaByDateTime(String datamart, LocalDateTime dateTime);

    /**
     * Get the delta ok meta-data
     *
     * @param datamart - datamart name
     * @return delta ok metadata
     */
    Future<OkDelta> getDeltaOk(String datamart);

    /**
     * Get the delta hot meta-data
     *
     * @param datamart - datamart name
     * @return delta hot metadata
     */
    Future<HotDelta> getDeltaHot(String datamart);

    /**
     * Get Write operations list
     *
     * @param datamart - datamart name
     * @return List of write operations
     */
    Future<List<DeltaWriteOp>> getDeltaWriteOperations(String datamart);
}