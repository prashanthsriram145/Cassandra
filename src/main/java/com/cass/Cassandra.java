package com.cass;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.UUID;

public class Cassandra {
    public static void main(String[] args) {
        Cassandra cassandra = new Cassandra();
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        Session session = cluster.connect("essentials");
        cassandra.writeData(session);
        cassandra.readData(session);
    }

    private void writeData(Session session) {
        PreparedStatement preparedStatement = session.prepare("insert into movies_by_actor(movie_id, title, release_year, actor) values (?,?,?,?)");
        BoundStatement bs = preparedStatement.bind(UUID.randomUUID(), "Spiderman", 9, "Steven");
        session.execute(bs);
    }

    private void readData(Session session) {
        ResultSet resultSet = session.execute("select * from movies_by_actor");
        System.out.println("Printing");
        for (Row row : resultSet) {
            System.out.println(String.format("%s, %s, %s", row.getUUID("movie_id"), row.getString("title"), row.getInt("release_year")));
        }
    }
}
