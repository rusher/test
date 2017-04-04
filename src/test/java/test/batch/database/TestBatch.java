package test.batch.database;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * test to create batch load.
 * table def
 *
 * CREATE TABLE `test_batch` (
 * `id` bigint(20) NOT NULL AUTO_INCREMENT,
 * `string` varchar(128) DEFAULT NULL,
 * `integer` int(11) NOT NULL,
 * PRIMARY KEY (`id`)
 * ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

 Hitting node directly threads properly terminate.

 Error encountered when going through proxy, some threads would lock up on trying to read response data

 [junit] Query is: INSERT INTO test_batch(string, `integer`) VALUES (?,?), parameters ['XFt7JyCDGbp5ymxaPg5cKCsop5KrU
 XJEYYM47IruaB6PlmkM2rzhhs3rDP0EFdbCBcGdNJ9NhEM8rVsF85XMKHtB1gjFb3224UMaDMT3YmMb1JgPuGKTJFsHFByRREDW',0],['BcmHGyCCUuixdE
 xTjAqHT5nUgc2gLdDXjT3IRL2gDmysWeO353SM7ivskVciXj6dcHXT5EF4OJnKPU0OlXLeUmDdlsszZRhZLK9P1vA6YugAF5eW6uF05XKAhainYFMR',1],[
 'bxMHHnkGr6iNMSZbIvTbIHsDaR4S6DZ9n499TCYt53uzxkinCZbyssu0B7y1QHOmMGI0s4kXLnmHp1KMemsFYG5snkTdooeRJqYEMm0k1riAusMnFM0TzLJ
 EolUwPSoe',2],['sSflDRk2wwJRS7gzn4oHosGQYge0f6wT2mtLnkmLoBZ311CdoqhmcHpHcNtc0EMgnPUpgG8ktHHsGMjr0BedZWnTnNNPy1lt51fqlKb2
 DnztxoqUA42yg7cdzv8gnjLW',3],['dMMeMm0kAxaFk4VhFTeD85qKPVdZFOYW1DtAAShU6lRccT4Z5V8wyLmCjQfjBUlPGaqlLwGj07MVrM5ixsNiktVva
 Vlzk3AVeOEhkm5en6fsmVkvCzytv42BO5A5M3m6',4],['5XisGHxez8R6sldYp9Z4mT0qDPzDe1LBNBAVLkj50lNMlK6oH5slZOG66cP8U8qnFn9Qu7jSXw
 1Di5pnCHLDGVK3dpx8aYJgNOg1XtqFr0wuOLen5ZvhICu44B9IoLlP',5],['2Kd9ClfN5cwKE52JyjDKz5CjgkQv7tHFvTC17e1SqDaPZ3Esh4ptGzwTvy2
 XpZ7EzfqNsut4X9d4mooigMZfqsYleB3XCAbCPOrqnCkbEda1CGJzEpZKaG0yKvPlCfTl',6],['LbOkj7V...
 [junit]     at org.mariadb.jdbc.internal.protocol.AbstractQueryProtocol.getResult(AbstractQueryProtocol.java:1064)
 [junit]     at org.mariadb.jdbc.internal.protocol.AsyncMultiRead.call(AsyncMultiRead.java:84)
 [junit]     at org.mariadb.jdbc.internal.protocol.AsyncMultiRead.call(AsyncMultiRead.java:12)
 [junit]     at java.util.concurrent.FutureTask.run(FutureTask.java:266)
 [junit]     at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142)
 [junit]     at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)
 [junit]     at java.lang.Thread.run(Thread.java:745)
 [junit] Caused by: java.io.EOFException: unexpected end of stream, read 0 bytes from 4
 [junit]     at org.mariadb.jdbc.internal.packet.read.ReadPacketFetcher.getReusableBuffer(ReadPacketFetcher.java:178)

 [junit]     at org.mariadb.jdbc.internal.protocol.AbstractQueryProtocol.getResult(AbstractQueryProtocol.java:1055)
 [junit]     ... 6 more
 [junit] java.lang.RuntimeException: runner failed
 [junit]     at test.outboundengine.database.TestBatch$Batcher.run(TestBatch.java:58)
 [junit] Caused by: java.sql.BatchUpdateException: (conn:3238) Could not read resultset: unexpected end of stream, re
 ad 0 bytes from 4
 */
public class TestBatch {

    protected static final String TRUNCATE = "truncate test_batch";

    protected static final String COUNT = "select count(*) from test_batch where `integer`=?";

    protected static final String INSERT = "INSERT INTO test_batch(string, `integer`) VALUES (?,?)";

    /** jdbc connection url */
    protected static final String URL = "jdbc:mariadb://127.0.0.1/test_batch?" +
            "user=maxscale&password=maxscale&" +
            "useServerPrepStmts=true&prepStmtCacheSize=250&prepStmtCacheSqlLimit=2048&autoReconnect=true";

    protected static final String NODE_URL = "jdbc:mariadb://127.0.0.1/test_batch?" +
            "user=maxscale&password=maxscale&" +
            "useServerPrepStmts=true&prepStmtCacheSize=250&prepStmtCacheSqlLimit=2048&autoReconnect=true";

    /** runner for testing multi thread batch update */
    class Batcher extends Thread {

        boolean finished = false;

        public Batcher(ThreadGroup tg, String name){
            super(tg, name);
        }

        /**
         * see if thread is finished
         * @return true if finished
         */
        public boolean isFinished() {
            return finished;
        }

        @Override
        public void run(){
            Connection conn = null;
            PreparedStatement stmt = null;
            try {
                conn = DriverManager.getConnection(URL);
                //conn = DriverManager.getConnection(NODE_URL);
                stmt = conn.prepareStatement(INSERT);
                buildBatchInsert(stmt);
                int[] updateCnts = stmt.executeBatch();
                conn.commit();
                Assert.assertEquals("didn't do all batch??? " + updateCnts.length, updateCnts.length, 100);
            } catch (Exception e) {
                throw new RuntimeException("runner failed",e);
            } finally {
                finished = true;
                try {
                    if(stmt != null) {
                        stmt.close();
                    }
                    if(conn != null) {
                        conn.close();
                    }
                } catch (Exception e) {
                    throw new RuntimeException("close failed", e);
                }
            }
        }

        /**
         * build batch sql
         * @param statement the statement to add batch sql update to
         * @throws SQLException if something goes pare shaped
         */
        protected void buildBatchInsert(PreparedStatement statement) throws SQLException {
            for(int cnt = 0; cnt < 100; cnt++) {
                statement.setString(1, RandomStringUtils.randomAlphanumeric(128));
                statement.setInt(2, cnt);
                statement.addBatch();
            }
        }
    };

    /**
     * truncate table data prior to doing batch load
     */
    protected void truncate(){
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DriverManager.getConnection(URL);
            //conn = DriverManager.getConnection(NODE_URL);
            stmt = conn.prepareStatement(TRUNCATE);
            stmt.execute();
            conn.commit();
        } catch (Exception e) {
            throw new RuntimeException("runner failed",e);
        } finally {
            try {
                if(stmt != null) {
                    stmt.close();
                }
                if(conn != null) {
                    conn.close();
                }
            } catch (Exception e) {
                throw new RuntimeException("close failed", e);
            }
        }
    }

    /**
     * assert that all rows were inserted
     */
    protected void assertCnt(int cnt){
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DriverManager.getConnection(URL);
            //conn = DriverManager.getConnection(NODE_URL);
            stmt = conn.prepareStatement(COUNT);
            stmt.setInt(1, cnt);
            rs = stmt.executeQuery();
            Assert.assertTrue("should've got count " + cnt, rs.next());
            Assert.assertEquals("should be 100 for " + cnt, rs.getLong(1), 100l);
        } catch (Exception e) {
            throw new RuntimeException("runner failed",e);
        } finally {
            try {
                if(rs != null) {
                    rs.close();
                }
                if(stmt != null) {
                    stmt.close();
                }
                if(conn != null) {
                    conn.close();
                }
            } catch (Exception e) {
                throw new RuntimeException("close failed", e);
            }
        }
    }

    @Test
    public void test() throws Exception{

        truncate();

        ThreadGroup tg = new ThreadGroup("batch_test");

        List<Batcher> threads = new ArrayList<>();

        for(int i = 0; i < 100; i++){
            Batcher b = new Batcher(tg, "batcher-" + i);
            b.start();
            threads.add(b);
        }

        Thread.sleep(1000);

        boolean done = false;
        while(!done) {
            done = true;
            for (Batcher b : threads) {
                if(!b.isFinished()) {
                    done = false;
                }
            }
            Thread.sleep(200);
        }
        for(int i = 0; i < 100; i++) {
            assertCnt(i);
        }
    }
}