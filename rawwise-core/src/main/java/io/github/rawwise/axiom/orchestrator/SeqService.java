package io.github.rawwise.axiom.orchestrator;

public interface SeqService {
    long nextVal(String seqId) throws CoordException;

    long curVal(String seqId) throws CoordException;

    SeqHandle create(String seqId, long initialValue) throws CoordException;

    boolean reset(String seqId, long value) throws CoordException;
}
