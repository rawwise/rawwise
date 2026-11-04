package io.github.rawwise.benchmark.vbench;

import java.util.List;

/**
 * VBench video generation evaluation prompt dataset.
 *
 * <p>Real VBench contains thousands of prompts across 16 dimensions. This class provides a small
 * representative subset, covering 5 categories: character/scene/action/physics/common sense, for
 * E2E testing and quick comparison experiments.
 *
 * <p>Production can replace with full VBench prompt list (loaded from
 * vbench-prompts/prompts/*.txt).
 */
public final class VideoPromptDataset {

    private VideoPromptDataset() {}

    public static List<String> smallSet() {
        return List.of(
                // Human (subject consistency, 6 items)
                "A young woman in a red jacket walking down a busy street",
                "An elderly man playing chess in a park, sunlight on his face",
                "A child laughing while running through a field of flowers",
                "A chef in white uniform preparing sushi in a kitchen",
                "A musician playing acoustic guitar on a wooden stage",
                "A scientist in a lab coat examining a glowing test tube",

                // Scene (background consistency, 4 items)
                "A bustling night market with neon lights and street food",
                "A serene mountain lake at sunrise with mist rising",
                "An ancient library with tall bookshelves and warm lamplight",
                "A futuristic city skyline with flying vehicles",

                // Motion (motion smoothness, 4 items)
                "A horse galloping across an open prairie",
                "Ocean waves crashing against rocky cliffs",
                "A ballerina spinning gracefully on stage",
                "Birds flying in formation across a clear blue sky",

                // Physics (physical realism, 3 items)
                "A glass cup falling and shattering on tile floor",
                "Water pouring from a pitcher into a wine glass",
                "Autumn leaves falling and swirling in the wind",

                // Long sequence (temporal consistency, 3 items)
                "A flower blooming from bud to full bloom over time",
                "A person walking from a sunny street into a dark cafe",
                "A cat slowly stalking and pouncing on a butterfly");
    }

    public static List<String> miniSet() {
        return List.of(
                "A young woman in a red jacket walking down a street",
                "A horse galloping across an open prairie",
                "A glass cup falling and shattering on tile floor");
    }
}
