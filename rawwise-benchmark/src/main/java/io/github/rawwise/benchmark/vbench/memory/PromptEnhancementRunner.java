package io.github.rawwise.benchmark.vbench.memory;

public final class PromptEnhancementRunner {
    private PromptEnhancementRunner() {}

    public static void main(String[] args) {
        String prompt =
                args.length > 0
                        ? String.join(" ", args)
                        : "A young woman in a red jacket walking down a street";
        String axiomBaseUrl =
                System.getProperty(
                        "rawwise.axiom.base-url",
                        System.getenv()
                                .getOrDefault("RAWWISE_AXIOM_BASE_URL", "http://localhost:8080"));
        PromptEnhancementPlanner planner =
                new PromptEnhancementPlanner(new RawWisePromptMemoryClient(axiomBaseUrl));
        PromptEnhancement enhancement = planner.enhance(prompt);
        System.out.println("=== Original Prompt ===");
        System.out.println(enhancement.originalPrompt());
        System.out.println("\n=== Enhanced Prompt ===");
        System.out.println(enhancement.enhancedPrompt());
        System.out.println("\nConstraints: " + enhancement.constraints().size());
        enhancement.constraints().forEach(c -> System.out.println("- " + c));
        System.out.println("Source memories: " + enhancement.sourceMemoryIds());
    }
}
