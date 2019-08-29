package com.github.hcsp;

import com.google.common.collect.ImmutableMap;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DatabaseAccessor {
    private final SqlSessionFactory sqlSessionFactory;

    public DatabaseAccessor() {
        try {
            InputStream inputStream = Resources.getResourceAsStream("db/mybatis/config.xml");
            sqlSessionFactory =
                    new SqlSessionFactoryBuilder().build(inputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized String selectNextUrlFromDatabase() throws SQLException {
        try (SqlSession session = sqlSessionFactory.openSession(true)) {
            String nextUrl = session.selectOne("com.github.hcsp.MyMapper.selectNextAvailableLink");
            if (nextUrl == null) {
                return null;
            }

            session.delete("com.github.hcsp.MyMapper.deleteLink", nextUrl);

            return nextUrl;
        }
    }
//
//    public synchronized void updateDatabase(String link, String sql) throws SQLException {
//        try (PreparedStatement statement = connection.prepareStatement(sql)) {
//            statement.setString(1, link);
//            statement.executeUpdate();
//        } catch (SQLException e) {
//            if (!(e instanceof JdbcSQLIntegrityConstraintViolationException) || !e.toString().contains("Unique index")) {
//                throw e;
//            }
//        }
//    }


    public boolean isLinkProcessed(String link) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            return ((Integer) session.selectOne("com.github.hcsp.MyMapper.selectProcessedLink", link)) == 1;
        }
    }

    public void storeIntoDatabaseIfItIsNewsPage(String url, Document doc) throws SQLException {
        ArrayList<Element> articleTags = doc.select("article");
        if (!articleTags.isEmpty()) {
            for (Element articleTag : articleTags) {
                String title = articleTags.get(0).child(0).text();
                System.out.println(title);
                List<Element> paragraphs = articleTag.select(".art_content p");
                String content = paragraphs.stream().map(Element::text).collect(Collectors.joining("\n"));

                try (SqlSession session = sqlSessionFactory.openSession()) {
                    session.insert("com.github.hcsp.MyMapper.insertNews", new News(url, content, title));
                }
            }
        }
    }

    public void insertProcessedLink(String link) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            session.insert("com.github.hcsp.MyMapper.insertLink", ImmutableMap.of("tableName", "LINKS_ALREADY_PROCESSED", "link", link));
        } catch (Exception e) {
            if (!e.getMessage().contains("Unique index")) {
                throw e;
            }
        }
    }

    public void insertLinkToBeProcessed(String link) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            session.insert("com.github.hcsp.MyMapper.insertLink", ImmutableMap.of("tableName", "LINKS_TO_BE_PROCESSED", "link", link));
        } catch (Exception e) {
            if (!e.getMessage().contains("Unique index")) {
                throw e;
            }
        }
    }
}
