package io.github.rawwise.backend.spark;

import java.util.*;
import org.apache.spark.sql.connector.read.InputPartition;

public record RawWiseUpdateLogInputPartition(List<Map<String, Comparable<?>>> rows)
        implements InputPartition {}
