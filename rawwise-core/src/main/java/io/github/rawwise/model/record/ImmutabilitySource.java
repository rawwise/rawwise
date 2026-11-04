package io.github.rawwise.model.record;

public enum ImmutabilitySource {
    DEFAULT(0),
    PROMOTED(1),
    FEEDBACK(2),
    CREATED(3),
    MUTABLE(4);

    private final int order;

    ImmutabilitySource(int order) {
        this.order = order;
    }

    public boolean canPromoteTo(ImmutabilitySource target) {
        if (this == CREATED || target == CREATED) {
            return this == target;
        }
        if (target == MUTABLE) {
            return false;
        }
        if (this == MUTABLE) {
            return true;
        }
        return this.order <= target.order;
    }

    public boolean isImmutable() {
        return this != MUTABLE;
    }
}
