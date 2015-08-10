package com.talentica.hungryHippos.consumers.cassandra;

import com.datastax.driver.core.*;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.UUID;

/**
 * Created by abhinavn on 11/6/15.
 */
public class CassandraClient {

    private Cluster cluster;
    private static Session session;
    private static PreparedStatement f;

    public CassandraClient(String node){
        connect(node);
        createSchema();
    }

    public void connect(String node) {
        cluster = Cluster.builder()
                .addContactPoint(node).build();
        cluster.init();
        session = cluster.connect();
        f = session.prepare("INSERT INTO testKeyspace.testTable (id, content) VALUES (? , ?)");
    }

    private void createSchema() {
        session.execute("CREATE KEYSPACE IF NOT EXISTS testKeyspace WITH replication " +
                "= {'class':'SimpleStrategy', 'replication_factor':3};");

        session.execute(
                "CREATE TABLE IF NOT EXISTS testKeyspace.testTable (" +
                        "id uuid PRIMARY KEY," +
                        "content text," +
                        ");");
    }

    public static void loadData(String data) {
        BoundStatement bs = new BoundStatement(f);
        UUID uuid = UUID.randomUUID();
        bs.bind(uuid,data);
        session.executeAsync(bs);
    }

    public void close(){
        cluster.close();
    }
}
