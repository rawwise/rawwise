package io.github.rawwise.common.catalog;

import io.github.rawwise.common.exception.SchemaIncompatibleException;
import io.github.rawwise.model.record.ArrayType;
import io.github.rawwise.model.record.MapType;
import io.github.rawwise.model.record.RawWiseType;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.StructField;
import io.github.rawwise.model.record.StructType;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class SchemaValidator {

    public enum Compatibility {
        NONE,
        BACKWARD,
        FORWARD,
        FULL
    }

    private SchemaValidator() {}

    public static void validate(Schema current, Schema proposed, Compatibility strategy) {
        if (strategy == Compatibility.NONE) {
            return;
        }
        if (current == null) {
            return;
        }
        if (proposed == null) {
            throw new SchemaIncompatibleException("Proposed schema cannot be null.");
        }

        validatePrimaryKeyEvolution(current, proposed, strategy);

        Map<Integer, StructField> currentById = buildFieldIdIndex(current);
        Map<Integer, StructField> proposedById = buildFieldIdIndex(proposed);

        if (currentById.isEmpty()) {
            validateLegacy(current, proposed, strategy);
            return;
        }

        Set<Integer> allFieldIds = new HashSet<>();
        allFieldIds.addAll(currentById.keySet());
        allFieldIds.addAll(proposedById.keySet());

        for (int fieldId : allFieldIds) {
            StructField currentField = currentById.get(fieldId);
            StructField proposedField = proposedById.get(fieldId);

            if (currentField != null && proposedField != null) {
                validateExistingFieldCompatibility(currentField, proposedField, strategy);
            } else if (currentField != null) {
                StructField proposedByName = null;
                for (StructField pf : proposed.fields()) {
                    if (!pf.hasAssignedFieldId() && pf.name().equals(currentField.name())) {
                        proposedByName = pf;
                        break;
                    }
                }
                if (proposedByName != null) {
                    if (strategy == Compatibility.BACKWARD || strategy == Compatibility.FULL) {
                        throw new SchemaIncompatibleException(
                                "Field '"
                                        + currentField.name()
                                        + "' fieldId was removed (assigned→unassigned). "
                                        + strategy
                                        + " compatibility requires fieldId preservation.");
                    }
                    validateExistingFieldCompatibility(currentField, proposedByName, strategy);
                } else {
                    validateFieldRemoval(currentField, strategy);
                }
            } else {
                StructField currentByName = null;
                for (StructField cf : current.fields()) {
                    if (!cf.hasAssignedFieldId() && cf.name().equals(proposedField.name())) {
                        currentByName = cf;
                        break;
                    }
                }
                if (currentByName != null) {
                    validateExistingFieldCompatibility(currentByName, proposedField, strategy);
                } else {
                    validateFieldAddition(proposedField, strategy);
                }
            }
        }

        for (StructField pf : proposed.fields()) {
            if (!pf.hasAssignedFieldId()) {
                boolean existsInCurrentByName = false;
                for (StructField cf : current.fields()) {
                    if (!cf.hasAssignedFieldId() && cf.name().equals(pf.name())) {
                        existsInCurrentByName = true;
                        break;
                    }
                }
                if (!existsInCurrentByName) {
                    validateFieldAddition(pf, strategy);
                }
            }
        }

        for (StructField cf : current.fields()) {
            if (!cf.hasAssignedFieldId()) {
                StructField matchingProposed = null;
                for (StructField pf : proposed.fields()) {
                    if (!pf.hasAssignedFieldId() && pf.name().equals(cf.name())) {
                        matchingProposed = pf;
                        break;
                    }
                }
                if (matchingProposed == null) {
                    boolean migratedToAssigned = false;
                    for (StructField pf : proposed.fields()) {
                        if (pf.hasAssignedFieldId() && pf.name().equals(cf.name())) {
                            migratedToAssigned = true;
                            break;
                        }
                    }
                    if (!migratedToAssigned) {
                        validateFieldRemoval(cf, strategy);
                    }
                } else {
                    validateExistingFieldCompatibility(cf, matchingProposed, strategy);
                }
            }
        }
    }

    private static void validateLegacy(Schema current, Schema proposed, Compatibility strategy) {
        List<StructField> currentFields = current.fields();
        List<StructField> proposedFields = proposed.fields();
        int minSize = Math.min(currentFields.size(), proposedFields.size());

        for (int i = 0; i < minSize; i++) {
            StructField cf = currentFields.get(i);
            StructField pf = proposedFields.get(i);

            if (!cf.name().equals(pf.name())) {
                throw new SchemaIncompatibleException(
                        "Field name mismatch at position "
                                + i
                                + ": expected '"
                                + cf.name()
                                + "' but found '"
                                + pf.name()
                                + "'");
            }

            if (!isNullableCompatible(cf.dataType(), pf.dataType())) {
                throw new SchemaIncompatibleException(
                        "Field '"
                                + cf.name()
                                + "' nullable changed incompatibly from nullable="
                                + cf.dataType().isNullable()
                                + " to nullable="
                                + pf.dataType().isNullable()
                                + ".");
            }

            if (!isTypeWideningCompatible(cf.dataType(), pf.dataType())) {
                throw new SchemaIncompatibleException(
                        "Field '"
                                + cf.name()
                                + "' type changed incompatibly from "
                                + cf.dataType().name()
                                + " to "
                                + pf.dataType().name()
                                + ".");
            }
        }

        for (int i = minSize; i < proposedFields.size(); i++) {
            validateFieldAddition(proposedFields.get(i), strategy);
        }

        for (int i = minSize; i < currentFields.size(); i++) {
            validateFieldRemoval(currentFields.get(i), strategy);
        }
    }

    private static void validateExistingFieldCompatibility(
            StructField current, StructField proposed, Compatibility strategy) {
        if (!current.name().equals(proposed.name())) {
            throw new SchemaIncompatibleException(
                    "Field name mismatch: expected '"
                            + current.name()
                            + "' but found '"
                            + proposed.name()
                            + "'"
                            + (proposed.hasAssignedFieldId()
                                    ? " (fieldId=" + proposed.fieldId() + ")"
                                    : ""));
        }

        if (!isNullableCompatible(current.dataType(), proposed.dataType())) {
            throw new SchemaIncompatibleException(
                    "Field '"
                            + current.name()
                            + "' nullable changed incompatibly from nullable="
                            + current.dataType().isNullable()
                            + " to nullable="
                            + proposed.dataType().isNullable()
                            + ".");
        }

        if (!isTypeWideningCompatible(current.dataType(), proposed.dataType())) {
            throw new SchemaIncompatibleException(
                    "Field '"
                            + current.name()
                            + "' type changed incompatibly from "
                            + current.dataType().name()
                            + " to "
                            + proposed.dataType().name()
                            + ".");
        }
    }

    private static void validateFieldAddition(StructField proposed, Compatibility strategy) {
        if (strategy == Compatibility.BACKWARD || strategy == Compatibility.FULL) {
            if (!proposed.dataType().isNullable() && proposed.defaultValue().isEmpty()) {
                throw new SchemaIncompatibleException(
                        "New field '"
                                + proposed.name()
                                + "' must be nullable or have a default value for "
                                + strategy
                                + " compatibility.");
            }
        }
    }

    private static void validateFieldRemoval(StructField current, Compatibility strategy) {
        if (strategy == Compatibility.FORWARD) {
            if (!current.dataType().isNullable() && current.defaultValue().isEmpty()) {
                throw new SchemaIncompatibleException(
                        "Removed field '"
                                + current.name()
                                + "' must be nullable or have a default value for "
                                + strategy
                                + " compatibility.");
            }
            return;
        }
        throw new SchemaIncompatibleException(
                "Field '"
                        + current.name()
                        + "' was removed. "
                        + strategy
                        + " compatibility requires no field removal.");
    }

    private static void validatePrimaryKeyEvolution(
            Schema current, Schema proposed, Compatibility strategy) {
        if (current.primaryKeyConstraint().isPresent()) {
            if (proposed.primaryKeyConstraint().isEmpty()) {
                throw new SchemaIncompatibleException(
                        "Removing primary key constraint is not allowed under "
                                + strategy
                                + " compatibility.");
            }
            if (!current.primaryKeyConstraint()
                    .get()
                    .fieldNames()
                    .equals(proposed.primaryKeyConstraint().get().fieldNames())) {
                throw new SchemaIncompatibleException(
                        "Primary key field set change is not allowed under "
                                + strategy
                                + " compatibility.");
            }
        }
    }

    static Map<Integer, StructField> buildFieldIdIndex(Schema schema) {
        Map<Integer, StructField> index = new HashMap<>();
        for (StructField f : schema.fields()) {
            if (f.hasAssignedFieldId()) {
                index.put(f.fieldId(), f);
            }
        }
        return index;
    }

    static boolean isTypeWideningCompatible(RawWiseType current, RawWiseType proposed) {
        if (!current.getClass().equals(proposed.getClass())) {
            String cn = current.name();
            String pn = proposed.name();
            return ("int32".equals(cn) && "int64".equals(pn))
                    || ("float32".equals(cn) && "float64".equals(pn));
        }

        if (current instanceof ArrayType ca && proposed instanceof ArrayType pa) {
            if (ca.fixedSize() > 0 && pa.fixedSize() > 0 && ca.fixedSize() != pa.fixedSize()) {
                return false;
            }
            if (ca.fixedSize() > 0 && pa.fixedSize() == 0) {
                return false;
            }
            return isTypeWideningCompatible(ca.elementType(), pa.elementType());
        }
        if (current instanceof MapType cm && proposed instanceof MapType pm) {
            return isTypeWideningCompatible(cm.keyType(), pm.keyType())
                    && isTypeWideningCompatible(cm.valueType(), pm.valueType());
        }
        if (current instanceof StructType cs && proposed instanceof StructType ps) {
            return checkStructWideningCompatible(cs, ps);
        }

        if (current.name().startsWith("decimal(") && proposed.name().startsWith("decimal(")) {
            int[] curPS = parseDecimalPrecisionScale(current.name());
            int[] propPS = parseDecimalPrecisionScale(proposed.name());
            if (propPS[0] < curPS[0]) return false;
            if (propPS[1] < curPS[1]) return false;
        }
        return true;
    }

    static boolean isNullableCompatible(RawWiseType current, RawWiseType proposed) {
        if (current.isNullable() && !proposed.isNullable()) return false;
        if (current instanceof ArrayType ca && proposed instanceof ArrayType pa)
            return isNullableCompatible(ca.elementType(), pa.elementType());
        if (current instanceof MapType cm && proposed instanceof MapType pm)
            return isNullableCompatible(cm.keyType(), pm.keyType())
                    && isNullableCompatible(cm.valueType(), pm.valueType());
        if (current instanceof StructType cs && proposed instanceof StructType ps)
            return checkStructNullableCompatible(cs, ps);
        return true;
    }

    private static boolean checkStructWideningCompatible(StructType current, StructType proposed) {
        Map<Integer, StructField> proposedById = new HashMap<>();
        for (StructField f : proposed.fields()) {
            if (f.hasAssignedFieldId()) proposedById.put(f.fieldId(), f);
        }
        for (StructField cf : current.fields()) {
            if (!cf.hasAssignedFieldId()) continue;
            StructField pf = proposedById.get(cf.fieldId());
            if (pf == null) return false;
            if (!isNullableCompatible(cf.dataType(), pf.dataType())) return false;
            if (!isTypeWideningCompatible(cf.dataType(), pf.dataType())) return false;
        }
        return true;
    }

    private static boolean checkStructNullableCompatible(StructType current, StructType proposed) {
        Map<Integer, StructField> proposedById = new HashMap<>();
        for (StructField f : proposed.fields()) {
            if (f.hasAssignedFieldId()) proposedById.put(f.fieldId(), f);
        }
        for (StructField cf : current.fields()) {
            if (!cf.hasAssignedFieldId()) continue;
            StructField pf = proposedById.get(cf.fieldId());
            if (pf == null) return false;
            if (!isNullableCompatible(cf.dataType(), pf.dataType())) return false;
        }
        return true;
    }

    private static int[] parseDecimalPrecisionScale(String name) {
        String inner = name.substring("decimal(".length(), name.length() - 1);
        String[] parts = inner.split(",");
        return new int[] {Integer.parseInt(parts[0].trim()), Integer.parseInt(parts[1].trim())};
    }
}
