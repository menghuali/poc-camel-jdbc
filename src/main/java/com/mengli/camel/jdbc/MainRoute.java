package com.mengli.camel.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@SuppressWarnings("unchecked")
@Component
public class MainRoute extends RouteBuilder {
    @Override
    public void configure() throws Exception {
        from("timer://foo?period=3000").setBody(constant("SELECT * FROM SOURCE_TABLE")).to("spring-jdbc:srcDataSource")
                .split(body())
                .process(e -> {
                    Map<String, Object> body = e.getIn().getBody(Map.class);
                    Record r = new Record();
                    r.id = (Integer) body.get("RECORD_ID");
                    r.value = (String) body.get("RECORD_VALUE");
                    r.value += "-Copied";
                    e.getIn().setBody(r);
                }).log("Copying ${body}")
                .setBody(simple(
                        "INSERT INTO DEST_TABLE(RECORD_ID, RECORD_VALUE) VALUES('${body.id}','${body.value}')"))
                .to("spring-jdbc:destDataSource");
    }

    @ToString
    @Setter
    @Getter
    static class Record {
        private Integer id;
        private String value;
    }

    @PostConstruct
    public void createTables() throws SQLException {
        try (Connection conn = srcDataSource.getConnection();
                PreparedStatement createTable = conn.prepareStatement(
                        "CREATE TABLE SOURCE_TABLE (RECORD_ID integer, RECORD_VALUE VARCHAR(255))")) {
            createTable.execute();
        }
        try (Connection conn = srcDataSource.getConnection();
                PreparedStatement insert1 = conn.prepareStatement(
                        "INSERT INTO SOURCE_TABLE(RECORD_ID, RECORD_VALUE) VALUES(1, 'A')");
                PreparedStatement insert2 = conn.prepareStatement(
                        "INSERT INTO SOURCE_TABLE(RECORD_ID, RECORD_VALUE) VALUES(2, 'B')")) {
            insert1.execute();
            insert2.execute();
        }
    }

    @Qualifier("srcDataSource")
    @Autowired
    DataSource srcDataSource;
}
