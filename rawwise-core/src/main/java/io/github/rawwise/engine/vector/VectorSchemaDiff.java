package io.github.rawwise.engine.vector;

import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.StructField;
import java.util.ArrayList;
import java.util.List;

public sealed interface VectorSchemaDiff {

    enum VectorChangeType {
        NO_VECTOR_CHANGE,
        VECTOR_ADDED,
        VECTOR_REMOVED,
        VECTOR_DIMENSION_CHANGED,
        VECTOR_MODEL_CHANGED
    }

    VectorChangeType type();

    record NoChange() implements VectorSchemaDiff {
        @Override
        public VectorChangeType type() {
            return VectorChangeType.NO_VECTOR_CHANGE;
        }
    }

    record VectorAdded(String columnName, int dimension) implements VectorSchemaDiff {
        @Override
        public VectorChangeType type() {
            return VectorChangeType.VECTOR_ADDED;
        }
    }

    record VectorRemoved(String columnName) implements VectorSchemaDiff {
        @Override
        public VectorChangeType type() {
            return VectorChangeType.VECTOR_REMOVED;
        }
    }

    record DimensionChanged(String columnName, int oldDimension, int newDimension)
            implements VectorSchemaDiff {
        @Override
        public VectorChangeType type() {
            return VectorChangeType.VECTOR_DIMENSION_CHANGED;
        }
    }

    record ModelChanged(String columnName, String oldModel, String newModel)
            implements VectorSchemaDiff {
        @Override
        public VectorChangeType type() {
            return VectorChangeType.VECTOR_MODEL_CHANGED;
        }
    }

    static List<VectorSchemaDiff> compare(Schema oldSchema, Schema newSchema) {
        List<StructField> oldVectorFields = findVectorFields(oldSchema);
        List<StructField> newVectorFields = findVectorFields(newSchema);

        if (oldVectorFields.isEmpty() && newVectorFields.isEmpty()) return List.of(new NoChange());

        List<VectorSchemaDiff> diffs = new ArrayList<>();

        for (StructField nf : newVectorFields) {
            if (oldVectorFields.stream().noneMatch(of -> of.name().equals(nf.name()))) {
                diffs.add(new VectorAdded(nf.name(), extractDimension(nf)));
            }
        }

        for (StructField of : oldVectorFields) {
            if (newVectorFields.stream().noneMatch(nf -> nf.name().equals(of.name()))) {
                diffs.add(new VectorRemoved(of.name()));
            }
        }

        for (StructField nf : newVectorFields) {
            for (StructField of : oldVectorFields) {
                if (nf.name().equals(of.name())) {
                    int oldDim = extractDimension(of);
                    int newDim = extractDimension(nf);
                    if (oldDim != newDim) {
                        diffs.add(new DimensionChanged(nf.name(), oldDim, newDim));
                    }
                }
            }
        }

        for (StructField nf : newVectorFields) {
            for (StructField of : oldVectorFields) {
                if (nf.name().equals(of.name())) {
                    String oldModel = of.metadata().getOrDefault("embedding.model", "");
                    String newModel = nf.metadata().getOrDefault("embedding.model", "");
                    if (!oldModel.equals(newModel)) {
                        diffs.add(new ModelChanged(nf.name(), oldModel, newModel));
                    }
                }
            }
        }

        return diffs.isEmpty() ? List.of(new NoChange()) : diffs;
    }

    private static List<StructField> findVectorFields(Schema schema) {
        List<StructField> result = new ArrayList<>();
        for (StructField f : schema.fields()) {
            if ("vector".equals(f.metadata().getOrDefault("rawwise.column.type", ""))) {
                result.add(f);
            }
        }
        return result;
    }

    private static int extractDimension(StructField field) {
        return Integer.parseInt(field.metadata().getOrDefault("vector.dimension", "768"));
    }
}
