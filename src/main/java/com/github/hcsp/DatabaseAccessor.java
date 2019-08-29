package com.github.hcsp;

import org.h2.jdbc.JdbcSQLIntegrityConstraintViolationException;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DatabaseAccessor {
    private static final String USER_NAME = "root";
    private static final String PASSWORD = "root";
    private final Connection connection;

    public DatabaseAccessor() {
        try {
            connection = DriverManager.getConnection("jdbc:h2:file:/Users/zhb/Projects/xiedaimala-crawler/news", USER_NAME, PASSWORD);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized String selectNextUrlFromDatabase() throws SQLException {
        List<String> urls = loadUrlsFromDatabase("select link from LINKS_TO_BE_PROCESSED LIMIT 1");
        if (urls.isEmpty()) {
            return null;
        }

        String targeUrl = urls.get(0);
        updateDatabase(targeUrl, "delete from LINKS_TO_BE_PROCESSED where link = ?");
        return targeUrl;
    }

    public synchronized void updateDatabase(String link, String sql) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, link);
            statement.executeUpdate();
        } catch (SQLException e) {
            if (!(e instanceof JdbcSQLIntegrityConstraintViolationException) || !e.toString().contains("Unique index")) {
                throw e;
            }
        }
    }

    private List<String> loadUrlsFromDatabase(String sql) throws SQLException {
        List<String> results = new ArrayList<>();
        ResultSet resultSet = null;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            resultSet = statement.executeQuery();
            while (resultSet.next()) {
                results.add(resultSet.getString(1));
            }
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
        }
        return results;
    }


    public boolean isLinkProcessed(String link) throws SQLException {
        ResultSet resultSet = null;
        try (PreparedStatement statement = connection.prepareStatement("SELECT LINK from LINKS_ALREADY_PROCESSED where link = ?")) {
            statement.setString(1, link);
            resultSet = statement.executeQuery();
            while (resultSet.next()) {
                return true;
            }
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
        }
        return false;
    }

    public void storeIntoDatabaseIfItIsNewsPage(String url, Document doc) throws SQLException {
        ArrayList<Element> articleTags = doc.select("article");
        if (!articleTags.isEmpty()) {
            for (Element articleTag : articleTags) {
                String title = articleTags.get(0).child(0).text();
                System.out.println(title);
                List<Element> paragraphs = articleTag.select(".art_content p");
                String content = paragraphs.stream().map(Element::text).collect(Collectors.joining("\n"));

                try (PreparedStatement statement = connection.prepareStatement("insert into news (title, content, url, created_at, modified_at)values (?, ?, ?, now(), now())")) {
                    statement.setString(1, title);
                    statement.setString(2, content);
                    statement.setString(3, url);
                    statement.executeUpdate();
                }
            }
        }
    }

}
