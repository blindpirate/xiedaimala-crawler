package com.github.hcsp;

public class Main {
    static DatabaseAccessor databaseAccessor = new DatabaseAccessor();

    public static void main(String[] args) {
        int threadCount = Integer.parseInt(System.getProperty("threadCount", "1"));
        for (int i = 0; i < threadCount; ++i) {
            new Thread(new Crawler(databaseAccessor)).start();
        }
    }
}
