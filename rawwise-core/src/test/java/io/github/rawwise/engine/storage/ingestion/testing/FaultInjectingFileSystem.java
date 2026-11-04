package io.github.rawwise.engine.storage.ingestion.testing;

import java.io.IOException;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

public class FaultInjectingFileSystem extends FilterFileSystem {

    public enum FaultOp {
        CREATE,
        RENAME,
        DELETE,
        OPEN,
        GET_FILE_STATUS,
        LIST_STATUS,
        MKDIRS,
        EXISTS
    }

    public record InjectionRule(Predicate<Path> pathMatcher, FaultOp op, IOException fault) {}

    private final Map<FaultOp, List<InjectionRule>> rules = new EnumMap<>(FaultOp.class);

    public FaultInjectingFileSystem(FileSystem fs) {
        super(fs);
        for (FaultOp op : FaultOp.values()) {
            rules.put(op, new CopyOnWriteArrayList<>());
        }
    }

    public void injectFailure(InjectionRule rule) {
        rules.get(rule.op()).add(rule);
    }

    public void clearInjections() {
        rules.values().forEach(List::clear);
    }

    public void clearInjections(FaultOp op) {
        rules.get(op).clear();
    }

    private void checkInjection(Path path, FaultOp op) throws IOException {
        for (InjectionRule rule : rules.get(op)) {
            if (rule.pathMatcher().test(path)) {
                throw rule.fault();
            }
        }
    }

    @Override
    public FSDataOutputStream create(
            Path f,
            FsPermission permission,
            boolean overwrite,
            int bufferSize,
            short replication,
            long blockSize,
            Progressable progress)
            throws IOException {
        checkInjection(f, FaultOp.CREATE);
        return super.create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        checkInjection(src, FaultOp.RENAME);
        checkInjection(dst, FaultOp.RENAME);
        return super.rename(src, dst);
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        checkInjection(f, FaultOp.DELETE);
        return super.delete(f, recursive);
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        checkInjection(f, FaultOp.OPEN);
        return super.open(f, bufferSize);
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        checkInjection(f, FaultOp.GET_FILE_STATUS);
        return super.getFileStatus(f);
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        checkInjection(f, FaultOp.LIST_STATUS);
        return super.listStatus(f);
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        checkInjection(f, FaultOp.MKDIRS);
        return super.mkdirs(f, permission);
    }

    @Override
    public boolean exists(Path f) throws IOException {
        checkInjection(f, FaultOp.EXISTS);
        return super.exists(f);
    }
}
