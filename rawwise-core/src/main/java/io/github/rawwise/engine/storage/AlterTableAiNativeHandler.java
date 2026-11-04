package io.github.rawwise.engine.storage;

import io.github.rawwise.common.collection.iterator.CloseableIterator;
import io.github.rawwise.engine.storage.codec.NullableValueCodec;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.model.record.ImmutabilitySource;
import io.github.rawwise.model.record.Record;
import io.github.rawwise.model.record.Records;
import io.github.rawwise.model.record.impl.DefaultRecordCodec;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

@Slf4j
public class AlterTableAiNativeHandler {

    private final DynamicMmapStore store;
    private final ImmutablePromotionService promotionService;

    public AlterTableAiNativeHandler(
            DynamicMmapStore store, ImmutablePromotionService promotionService) {
        this.store = store;
        this.promotionService = promotionService;
    }

    public PromotionResult promoteAll(String tableIdentifier, long tableId) {
        log.info("ALTER TABLE ai-native promotion started: table={}", tableIdentifier);
        long start = System.currentTimeMillis();

        long promoted = 0;
        long skipped = 0;

        store.setTableAiNative(tableIdentifier, true);

        TableStore ts = store.tableStore(tableIdentifier);
        for (LmdbInstance instance : ts.streamInstances().toList()) {
            try (CloseableIterator<Pair<byte[], byte[]>> iter = instance.closeableIterator()) {
                while (iter.hasNext()) {
                    Pair<byte[], byte[]> entry = iter.next();
                    byte[] keyBytes = entry.getLeft();
                    byte[] valueBytes = entry.getRight();

                    if (valueBytes == null) continue;

                    if (NullableValueCodec.isTombstone(valueBytes)) {
                        skipped++;
                        continue;
                    }

                    byte[] decoded = NullableValueCodec.decode(valueBytes);
                    if (decoded == null) {
                        skipped++;
                        continue;
                    }

                    boolean isImmutable = DefaultRecordCodec.extractImmutableStatic(decoded);
                    if (isImmutable) {
                        skipped++;
                        continue;
                    }

                    ImmutabilitySource source =
                            DefaultRecordCodec.extractImmutabilitySourceStatic(decoded);
                    if (!promotionService.canPromote(source)) {
                        skipped++;
                        continue;
                    }

                    Record.Key key = Records.keyOf(keyBytes);
                    boolean success = promotionService.markImmutable(key, tableId);
                    if (success) {
                        promoted++;
                    } else {
                        skipped++;
                    }
                }
            } catch (Exception e) {
                log.warn(
                        "ALTER TABLE ai-native: failed to iterate instance for table={}: {}",
                        tableIdentifier,
                        e.getMessage());
            }
        }

        log.info(
                "ALTER TABLE ai-native promotion finished: table={}, promoted={}, skipped={}, elapsed={}ms",
                tableIdentifier,
                promoted,
                skipped,
                System.currentTimeMillis() - start);

        return new PromotionResult(promoted, skipped, System.currentTimeMillis() - start);
    }

    public record PromotionResult(long promoted, long skipped, long elapsedMs) {}
}
