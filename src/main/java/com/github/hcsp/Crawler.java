package com.github.hcsp;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.sql.SQLException;

public class Crawler implements Runnable {

    private final DatabaseAccessor databaseAccessor;

    public Crawler(DatabaseAccessor databaseAccessor) {
        this.databaseAccessor = databaseAccessor;
    }

    @Override
    public void run() {
        String link;
        try {
            while ((link = databaseAccessor.selectNextUrlFromDatabase()) != null) {
                // 询问数据库，当前链接是不是已经被处理过了？
                if (databaseAccessor.isLinkProcessed(link)) {
                    continue;
                }

                if (isInterestingLink(link)) {
                    System.out.println(String.format("Thread name: %s, processing link: %s", Thread.currentThread().getName(), link));

                    Document doc = httpGetAndParseHtml(link);

                    parseUrlsFromPageAndStoreIntoDatabase(doc);

                    databaseAccessor.storeIntoDatabaseIfItIsNewsPage(link, doc);

                    databaseAccessor.updateDatabase(link, "INSERT INTO LINKS_ALREADY_PROCESSED (link) values (?)");
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void parseUrlsFromPageAndStoreIntoDatabase(Document doc) throws SQLException {
        for (Element aTag : doc.select("a")) {
            String href = aTag.attr("href");
            if (StringUtils.isBlank(href) || href.toLowerCase().startsWith("javascript") || href.startsWith("#") || href.startsWith("?")) {
                continue;
            }
            if (href.startsWith("//")) {
                href = "https:" + href;
            }
            databaseAccessor.updateDatabase(href, "INSERT INTO LINKS_TO_BE_PROCESSED (link) values (?)");
        }
    }

    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    private static Document httpGetAndParseHtml(String link) throws IOException {
        // 这是我们感兴趣的，我们只处理新浪站内的链接
        CloseableHttpClient httpclient = HttpClients.createDefault();

        if (link.startsWith("//")) {
            link = "https:" + link;
        }

        HttpGet httpGet = new HttpGet(link);
        httpGet.addHeader("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36");

        try (CloseableHttpResponse response1 = httpclient.execute(httpGet)) {
            HttpEntity entity1 = response1.getEntity();
            String html = EntityUtils.toString(entity1);
            return Jsoup.parse(html);
        }
    }

    // 我们只关心news。sina的，我们要排除登陆页面
    private static boolean isInterestingLink(String link) {
        return (isNewsPage(link) || isIndexPage(link)) && isNotLoginPage(link);
    }

    private static boolean isIndexPage(String link) {
        return "https://sina.cn".equals(link);
    }

    private static boolean isNewsPage(String link) {
        return link.contains("news.sina.cn");
    }

    private static boolean isNotLoginPage(String link) {
        return !link.contains("passport.sina.cn");
    }
}
