package io.github.rawwise.axiom.memory;

/** 语义栈下钻深度：ABSTRACT=摘要（View），DETAIL=原文（Raw 正文）。 */
public enum Depth {
    ABSTRACT,
    DETAIL;

    /**
     * 解析深度字符串，兼容 legacy L0/L1/L2 载入层。 缺省→ABSTRACT（防
     * NPE，保守默认返回摘要层）；L0（精炼摘要层）→ABSTRACT；L1/L2（细节层）→DETAIL。
     */
    public static Depth resolve(String raw) {
        if (raw == null || raw.isBlank()) {
            return ABSTRACT;
        }
        String upper = raw.trim().toUpperCase();
        return switch (upper) {
            case "ABSTRACT", "L0" -> ABSTRACT;
            case "DETAIL", "L1", "L2" -> DETAIL;
            default ->
                    throw new IllegalArgumentException(
                            "Invalid depth '" + raw + "', expected ABSTRACT|DETAIL");
        };
    }
}
