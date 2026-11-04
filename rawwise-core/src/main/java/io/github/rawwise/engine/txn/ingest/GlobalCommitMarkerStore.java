package io.github.rawwise.engine.txn.ingest;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * GlobalCommitMarker persistencestorageinterface.
 *
 * <p>will {@link GlobalCommitMarker} persistenceto Metastore, via CAS create(precondition:
 * NotExists).
 *
 * <p>Metastore key pattern: /commit_marker/{catalog}/{db}/{table}/{batchId}
 *
 * <p>CAS semantics:
 *
 * <ul>
 *   <li>{@link #create} — precondition: NotExists(key), ensure marker onlycreateonetime
 * </ul>
 *
 * <p>concurrentcontention(E2E-006b): two batch while CAS sameone marker → onlyonesuccess,
 * failpartyreturn false.
 *
 * <p>Implementation class: {@link MetastoreGlobalCommitMarkerStore}
 */
public interface GlobalCommitMarkerStore {

    /**
     * via CAS create GlobalCommitMarker(precondition: NotExists).
     *
     * <p>thisisRead pathvisibleityuniqueswitch.marker createsuccess = notrollbackboundary.
     *
     * @param marker needcreate GlobalCommitMarker
     * @return true if CAS success(firstcreate), false ifalreadyexistsame batchId marker
     * @throws IOException if Metastore readwritefail
     */
    boolean create(GlobalCommitMarker marker) throws IOException;

    /**
     * by batchId load GlobalCommitMarker.
     *
     * <p>byin key include catalog/db/table path, thismethodvia prefix scan + suffix
     * matchimplementation.
     *
     * @param batchId batch ID
     * @return GlobalCommitMarker(ifexist), otherwise Optional.empty()
     * @throws IOException if Metastore readwritefail
     */
    Optional<GlobalCommitMarker> load(String batchId) throws IOException;

    /**
     * bytableloadall GlobalCommitMarker.
     *
     * <p>forcrash recoverywhen scanning a table'sallcommitmark.
     *
     * @param catalog catalog name
     * @param database Database name
     * @param table table name
     * @return GlobalCommitMarker list(canenergyis empty)
     * @throws IOException if Metastore readwritefail
     */
    List<GlobalCommitMarker> loadByTable(String catalog, String database, String table)
            throws IOException;
}
