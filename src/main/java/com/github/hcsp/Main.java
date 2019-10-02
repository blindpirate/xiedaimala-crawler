package com.github.hcsp;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class Main {
    public static void main(String[] args) {
//        CrawlerDao dao = new MyBatisCrawlerDao();
//
//        for (int i = 0; i < 8; ++i) {
//           new Crawler(dao).start();
//        }
        long t0 = System.currentTimeMillis();
        slow();
        new Thread(Main::slow).start();
        new Thread(Main::slow).start();
        new Thread(Main::slow).start();
        System.out.println(System.currentTimeMillis() - t0);
    }

    private static void slow() {
        try {
            File tmp = File.createTempFile("tmp", "");
            for (int i = 0; i < 10000; i++) {
                try (FileOutputStream fos = new FileOutputStream(tmp)) {
                    fos.write(i);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
