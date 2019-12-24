package com.sql.exec.perform;

import java.lang.reflect.Field;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.joda.time.DateTime;
import org.sql2o.Query;
import org.sql2o.Sql2o;
import org.sql2o.tools.FeatureDetector;

/**
 * Date: 2019-12-18
 */
public class PerformanceTestSetup {
    private final static String DRIVER_CLASS = "org.h2.Driver";
    private final static String DB_URL = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1";
    private final static String DB_USER = "sa";
    private final static String DB_PASSWORD = "";
    private final static String HIBERNATE_DIALECT = "org.hibernate.dialect.H2Dialect";
    private int ITERATIONS = 100000;

    public Sql2o getSql2o() {
        return sql2o;
    }

    private Sql2o sql2o;

    public void setup(int iteration) {
        this.ITERATIONS = iteration;
        setup();
    }

    public void setup() {
        Logger.getLogger("org.hibernate").setLevel(Level.OFF);
        sql2o = new Sql2o(DB_URL, DB_USER, DB_PASSWORD);

        createPostTable();

        // turn off oracle because ResultSetUtils slows down with oracle
        setOracleAvailable(false);
    }


    private void createPostTable() {
        sql2o.createQuery("DROP TABLE IF EXISTS post").executeUpdate();

        sql2o.createQuery("\n CREATE TABLE post" +
            "\n (" +
            "\n     id INT NOT NULL IDENTITY PRIMARY KEY" +
            "\n   , text VARCHAR(255)" +
            "\n   , creation_date DATETIME" +
            "\n   , last_change_date DATETIME" +
            "\n   , counter1 INT" +
            "\n   , counter2 INT" +
            "\n   , counter3 INT" +
            "\n   , counter4 INT" +
            "\n   , counter5 INT" +
            "\n   , counter6 INT" +
            "\n   , counter7 INT" +
            "\n   , counter8 INT" +
            "\n   , counter9 INT" +
            "\n )" +
            "\n;").executeUpdate();

        Random r = new Random();

        Query insQuery = sql2o.createQuery("insert into post (text, creation_date, last_change_date, counter1, counter2, counter3, counter4, counter5, counter6, counter7, counter8, counter9) values (:text, :creation_date, :last_change_date, :counter1, :counter2, :counter3, :counter4, :counter5, :counter6, :counter7, :counter8, :counter9)");
        for (int idx = 0; idx < ITERATIONS; idx++)
        {
            insQuery.addParameter("text", "a name " + idx)
                .addParameter("creation_date", new DateTime(System.currentTimeMillis() + r.nextInt()).toDate())
                .addParameter("last_change_date", new DateTime(System.currentTimeMillis() + r.nextInt()).toDate())
                .addParameter("counter1", r.nextDouble() > 0.5 ? r.nextInt() : null)
                .addParameter("counter2", r.nextDouble() > 0.5 ? r.nextInt() : null)
                .addParameter("counter3", r.nextDouble() > 0.5 ? r.nextInt() : null)
                .addParameter("counter4", r.nextDouble() > 0.5 ? r.nextInt() : null)
                .addParameter("counter5", r.nextDouble() > 0.5 ? r.nextInt() : null)
                .addParameter("counter6", r.nextDouble() > 0.5 ? r.nextInt() : null)
                .addParameter("counter7", r.nextDouble() > 0.5 ? r.nextInt() : null)
                .addParameter("counter8", r.nextDouble() > 0.5 ? r.nextInt() : null)
                .addParameter("counter9", r.nextDouble() > 0.5 ? r.nextInt() : null)
                .addToBatch();
        }
        insQuery.executeBatch();
    }

    private void setOracleAvailable(boolean b) {
        try {
            Field f = FeatureDetector.class.getDeclaredField("oracleAvailable");
            f.setAccessible(true);
            f.set(null, b);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }
}
