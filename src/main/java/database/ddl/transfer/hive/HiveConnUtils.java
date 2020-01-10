package database.ddl.transfer.hive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.*;
import java.util.*;
/**
 *@ClassName HiveConnUtils
 *@Description TODO
 *@Author luoyuntian
 *@Date 2020-01-03 15:38
 *@Version
 **/

public class HiveConnUtils {
    private final static List<Connection> pool = new ArrayList<Connection>();
    private static final Integer MAX_CONNECTION_NUM = 1;
    static {
        Properties p = new Properties();
        try{
            p.load(HiveConnUtils.class.getClassLoader().getResourceAsStream("jdbc.properties"));
            String driver = p.getProperty("hive.driverClass");
            String url = p.getProperty("hive.url");
            String user = p.getProperty("hive.user.name");
            String password = p.getProperty("hive.user.password");
            Class.forName(driver);
            for(int i=0;i<MAX_CONNECTION_NUM;i++){
                Connection con = DriverManager.getConnection(url,user,password);
                pool.add(con);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    /**
       * @author luoyuntian
       * @date 2020-01-03 15:40
       * @description 获取连接
        * @param
       * @return
       */
    public static synchronized Connection getConnection(){
        Logger logger = LoggerFactory.getLogger(HiveConnUtils.class);
        if(pool.size()<=0){
            logger.info("hive连接池为0");
            try{
                Thread.sleep(100);
            }catch (Exception e){
                logger.error(e.getMessage());
            }
            return getConnection();
        }

        return pool.remove(0);
    }
    /**
       * @author luoyuntian
       * @date 2020-01-03 15:40
       * @description 归还连接
        * @param
       * @return
       */
    public static void back(Connection con){
        pool.add(con);
    }


    /**
       * @author luoyuntian
       * @date 2020-01-08 15:56
       * @description 根据配置创建hive连接
        * @param
       * @return
       */
    public static Connection getHiveConnection(String url,String driver,String userName,String password){
        Connection con = null;
        try {
           Class.forName(driver);
           con = DriverManager.getConnection(url,userName,password);
       }catch (Exception e){
           e.printStackTrace();
       }
        return con;
    }
    /**
       * @author luoyuntian
       * @date 2020-01-08 16:38
       * @description TODO
        * @param
       * @return
       */
    public static void closeConnection(Connection con){
        try{
            con.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
