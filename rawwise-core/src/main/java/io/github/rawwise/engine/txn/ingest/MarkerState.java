package io.github.rawwise.engine.txn.ingest;

/**
 * GlobalCommitMarker stateenum.
 *
 * <p>semantics:
 *
 * <ul>
 *   <li>{@link #COMMITTED} — globalcommitmarkalreadywrite, Read pathvisiblethisbatchdata
 *   <li>{@link #ABORTED} — batchalreadyinstop, datainvisible
 * </ul>
 *
 * <p>terminal state: twostateallasterminal state, notagaintransition.
 */
public enum MarkerState {

    /** globalcommitmarkalreadywrite, Read pathvisiblethisbatchdata */
    COMMITTED,

    /** batchalreadyinstop, datainvisible */
    ABORTED
}
