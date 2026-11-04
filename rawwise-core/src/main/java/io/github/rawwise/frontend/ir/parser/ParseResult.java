package io.github.rawwise.frontend.ir.parser;

import io.github.rawwise.frontend.ir.QueryIR;

public sealed interface ParseResult permits ParseResult.IRResult, ParseResult.FallbackResult {

    record IRResult(QueryIR ir) implements ParseResult {}

    record FallbackResult(String reason, String originalSql) implements ParseResult {}
}
