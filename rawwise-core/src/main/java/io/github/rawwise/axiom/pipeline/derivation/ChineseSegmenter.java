package io.github.rawwise.axiom.pipeline.derivation;

import com.huaban.analysis.jieba.JiebaSegmenter;
import com.huaban.analysis.jieba.SegToken;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Chinese word segmentation for improving embedding quality. Uses jieba segmentation to break
 * Chinese text into meaningful words, which yields better vector similarity than character-level
 * embedding.
 */
@Slf4j
public class ChineseSegmenter {

    private final JiebaSegmenter segmenter;
    private static volatile ChineseSegmenter instance;

    public ChineseSegmenter() {
        this.segmenter = new JiebaSegmenter();
    }

    public static ChineseSegmenter getInstance() {
        if (instance == null) {
            synchronized (ChineseSegmenter.class) {
                if (instance == null) {
                    instance = new ChineseSegmenter();
                }
            }
        }
        return instance;
    }

    /**
     * Segment text and return as space-separated words. Preserves non-Chinese tokens (code, URLs,
     * etc.) as-is.
     */
    public String segment(String text) {
        if (text == null || text.isEmpty()) {
            return text;
        }
        try {
            List<SegToken> tokens = segmenter.process(text, JiebaSegmenter.SegMode.SEARCH);
            return tokens.stream()
                    .map(t -> t.word)
                    .filter(w -> w != null && !w.isBlank())
                    .collect(Collectors.joining(" "));
        } catch (Exception e) {
            log.debug("jieba segmentation failed, returning original text: {}", e.getMessage());
            return text;
        }
    }

    /** Check if text contains significant Chinese characters. */
    public static boolean hasChinese(String text) {
        return text.codePoints()
                .anyMatch(
                        cp ->
                                (cp >= 0x4E00 && cp <= 0x9FFF)
                                        || // CJK Unified
                                        (cp >= 0x3400 && cp <= 0x4DBF)
                                        || // CJK Extension A
                                        (cp >= 0xF900 && cp <= 0xFAFF)); // CJK Compatibility
    }
}
