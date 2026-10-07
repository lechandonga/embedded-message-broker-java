package com.github.lechandonga.embeddedmessagebrokerjava.broker;

/** 测试日志：让每个用例在运行日志中体现覆盖的业务与结果。 */
public final class TestLog {

    private TestLog() {
    }

    public static synchronized void pass(String business, String caseName, String detail) {
        System.out.printf("[业务覆盖] %s | 用例: %s | %s | 结果: PASS%n", business, caseName, detail);
    }
}
