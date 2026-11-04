package io.github.rawwise.benchmark.ycsb;

public class YcsbTransactionTestRunner {
    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 8080;
        String database = args.length > 2 ? args[2] : "default";

        System.out.println("=== YCSB Transaction Pipeline Test ===");
        System.out.println("  Host: " + host + ":" + port);
        System.out.println("  Database: " + database);
        System.out.println("  Path: Test → HTTP → Frontend → Backend → LMDB\n");

        YcsbTransactionTest test = new YcsbTransactionTest(host, port, database);
        try {
            test.setup();
            boolean allPassed = true;

            allPassed &= test.testSequentialInsertThroughput();
            allPassed &= test.testSequentialSelectThroughput();
            allPassed &= test.testReadAfterWriteConsistency();
            allPassed &= test.testConcurrentInsert();
            allPassed &= test.testMixedSequentialReadWrite();

            System.out.println("\n========================================");
            System.out.println(allPassed ? "ALL TESTS PASSED" : "SOME TESTS FAILED");
            System.out.println("========================================");
        } finally {
            test.teardown();
        }
    }
}
