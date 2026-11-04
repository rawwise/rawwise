package io.github.rawwise.common;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Strs {
    public static String leftPad(String input, int fixedLength) {
        return IntStream.range(0, fixedLength - input.length())
                        .mapToObj(i -> "0")
                        .collect(Collectors.joining())
                + input;
    }
}
