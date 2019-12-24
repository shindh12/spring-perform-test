package com.sql.exec.perform;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.joda.time.DateTime;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.sql2o.Query;
import org.sql2o.Sql2o;
import org.sql2o.tools.FeatureDetector;

/**
 * @author aldenquimby@gmail.com
 *
 * TODO: must read 10-100 rows instead 1
 *
 */
public class PojoPerformanceTest
{
    private final static String DRIVER_CLASS = "org.h2.Driver";
    private final static String DB_URL = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1";
    private final static String DB_USER = "sa";
    private final static String DB_PASSWORD = "";
    private final static String HIBERNATE_DIALECT = "org.hibernate.dialect.H2Dialect";
    private final int ITERATIONS = 100000;

    private Sql2o sql2o;

    public PojoPerformanceTest(Sql2o sql2o) {
        this.sql2o = sql2o;
    }


    public List<PerformResult> select()
    {
        System.out.println("Running " + ITERATIONS + " iterations that load up a Post entity\n");

        PerformanceTestList tests = new PerformanceTestList();
        tests.add(new HandCodedSelect());
        tests.add(new HibernateTypicalSelect());
        tests.add(new MyBatisSelect());
        tests.add(new SpringJdbcTemplateSelect());

        System.out.println("Warming up...");
        tests.run(ITERATIONS);
        System.out.println("Done warming up, let's rock and roll!\n");

        tests.run(ITERATIONS);
        return tests.getResults("Select");
    }

    //----------------------------------------
    //          performance tests
    // ---------------------------------------

    // TODO I think we should consider making it a REQUIREMENT for the performance tests
    // that underscore case be mapped to camel case because it is so common.
    // This would allow us to remove the "optimized" sql2o select, which is not really
    // too different from the "typical" one...
    // If not, maybe we should entirely break out the underscore case mapping tests into a different
    // section in the readme...

    final static String SELECT_TYPICAL = "SELECT * FROM post";
    final static String SELECT_OPTIMAL = "SELECT id, text, creation_date as creationDate, last_change_date as lastChangeDate, counter1, counter2, counter3, counter4, counter5, counter6, counter7, counter8, counter9 FROM post";

    /**
     * Considered "optimized" because it uses {@link #SELECT_OPTIMAL} rather
     * than auto-mapping underscore case to camel case.
     */
    class Sql2oOptimizedSelect extends PerformanceTestBase
    {
        private org.sql2o.Connection conn;
        private Query query;

        @Override
        public void init()
        {
            conn = sql2o.open();
            query = conn.createQuery(SELECT_OPTIMAL + " WHERE id = :id");
        }

        @Override
        public void run(int input)
        {
            query.addParameter("id", input)
                 .executeAndFetchFirst(Post.class);
        }

        @Override
        public void close()
        {
            conn.close();
        }
    }

    class Sql2oTypicalSelect extends PerformanceTestBase
    {
        private org.sql2o.Connection conn;
        private Query query;

        @Override
        public void init()
        {
            conn = sql2o.open();
            query = conn.createQuery(SELECT_TYPICAL + " WHERE id = :id")
                    .setAutoDeriveColumnNames(true);
        }

        @Override
        public void run(int input)
        {
            query.addParameter("id", input)
                 .executeAndFetchFirst(Post.class);
        }

        @Override
        public void close()
        {
            conn.close();
        }
    }



    class HandCodedSelect extends PerformanceTestBase
    {
        private Connection conn = null;
        private PreparedStatement stmt = null;

        @Override
        public void init()
        {
            try {
                conn = sql2o.open().getJdbcConnection();
                stmt = conn.prepareStatement(SELECT_TYPICAL + " WHERE id = ?");
            }
            catch(SQLException se) {
                throw new RuntimeException("error when executing query", se);
            }
        }

        private Integer getNullableInt(ResultSet rs, String colName) throws SQLException {
            Object obj = rs.getObject(colName);
            return obj == null ? null : (Integer)obj;
        }

        @Override
        public void run(int input)
        {
            ResultSet rs = null;

            try {
                stmt.setInt(1, input);

                rs = stmt.executeQuery();

                while(rs.next()) {
                    Post p = new Post();
                    p.setId(rs.getInt("id"));
                    p.setText(rs.getString("text"));
                    p.setCreationDate(rs.getDate("creation_date"));
                    p.setLastChangeDate(rs.getDate("last_change_date"));
                    p.setCounter1(getNullableInt(rs, "counter1"));
                    p.setCounter2(getNullableInt(rs, "counter2"));
                    p.setCounter3(getNullableInt(rs, "counter3"));
                    p.setCounter4(getNullableInt(rs, "counter4"));
                    p.setCounter5(getNullableInt(rs, "counter5"));
                    p.setCounter6(getNullableInt(rs, "counter6"));
                    p.setCounter7(getNullableInt(rs, "counter7"));
                    p.setCounter8(getNullableInt(rs, "counter8"));
                    p.setCounter9(getNullableInt(rs, "counter9"));
                }
            }
            catch (SQLException e) {
                throw new RuntimeException("error when executing query", e);
            }
            finally {
                if (rs != null) {
                    try {
                        rs.close();
                    }
                    catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        public void close()
        {
            if(stmt != null) {
                try {
                    stmt.close();
                }
                catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                }
                catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    class HibernateTypicalSelect extends PerformanceTestBase
    {
        private Session session;

        @Override
        public void init()
        {
            Logger.getLogger("org.hibernate").setLevel(Level.OFF);

            Configuration cfg = new Configuration()
                    .setProperty("hibernate.connection.driver_class", DRIVER_CLASS)
                    .setProperty("hibernate.connection.url", DB_URL)
                    .setProperty("hibernate.connection.username", DB_USER)
                    .setProperty("hibernate.connection.password", DB_PASSWORD)
                    .setProperty("hibernate.dialect", HIBERNATE_DIALECT)
                    .setProperty("hbm2ddl.auto", "update")
                    .addAnnotatedClass(Post.class);
            cfg.setPhysicalNamingStrategy(new ImprovedNamingStrategy());


            ServiceRegistry serviceRegistry = new StandardServiceRegistryBuilder()
                    .applySettings(cfg.getProperties())
                    .build();

            SessionFactory sessionFactory = cfg.buildSessionFactory(serviceRegistry);
            session = sessionFactory.openSession();
        }

        @Override
        public void run(int input)
        {
            session.get(Post.class, input);
        }

        @Override
        public void close()
        {
            session.close();
        }
    }

    class MyBatisSelect extends PerformanceTestBase
    {
        private SqlSession session;

        @Override
        public void init()
        {
            TransactionFactory transactionFactory = new JdbcTransactionFactory();
            Environment environment = new Environment("development", transactionFactory, sql2o.getDataSource());
            org.apache.ibatis.session.Configuration config = new org.apache.ibatis.session.Configuration(environment);
            config.addMapper(MyBatisPostMapper.class);
            SqlSessionFactory sqlSessionFactory = new SqlSessionFactoryBuilder().build(config);
            session = sqlSessionFactory.openSession();
        }

        @Override
        public void run(int input)
        {
            session.getMapper(MyBatisPostMapper.class).selectPost(input);
        }

        @Override
        public void close()
        {
            session.close();
        }
    }

    /**
     * Mapper interface required for MyBatis performance test
     */
    interface MyBatisPostMapper
    {
        @Select(SELECT_TYPICAL + " WHERE id = #{id}")
        @Results({
            @Result(property = "creationDate", column = "creation_date"),
            @Result(property = "lastChangeDate", column = "last_change_date")
        })
        Post selectPost(int id);
    }

    class SpringJdbcTemplateSelect extends PerformanceTestBase
    {
        private NamedParameterJdbcTemplate jdbcTemplate;

        @Override
        public void init()
        {
            jdbcTemplate = new NamedParameterJdbcTemplate(sql2o.getDataSource());
        }

        @Override
        public void run(int input)
        {
            jdbcTemplate.queryForObject(SELECT_TYPICAL + " WHERE id = :id",
                                        Collections.singletonMap("id", input),
                                        new BeanPropertyRowMapper<Post>(Post.class));
        }

        @Override
        public void close()
        {}
    }
}
