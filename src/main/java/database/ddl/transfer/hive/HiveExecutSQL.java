package database.ddl.transfer.hive;
import database.ddl.transfer.consts.HiveKeyWord;
import database.ddl.transfer.consts.HiveStoreType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.*;
import java.util.*;
/**
 *@ClassName HiveExecutSQL
 *@Description TODO
 *@Author luoyuntian
 *@Date 2020-01-03 15:34
 *@Version
 **/
public class HiveExecutSQL {
    protected static Logger logger = LoggerFactory.getLogger(HiveExecutSQL.class);
    /**
       * @author luoyuntian
       * @date 2020-01-03 15:49
       * @description 根据表名获取建表语句
        * @param
       * @return
       */
    public static String getHiveCreateDDL(String databaseName,String tableName,Connection con){
//        Connection con = HiveConnUtils.getConnection();
        StringBuilder result = new StringBuilder();
        String sql = "show create table "+databaseName+"."+tableName;
        PreparedStatement ps = null;
        try{
            ps = con.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            while(rs.next()){
                result.append((String) rs.getObject(1));
            }
            rs.close();
        }catch (Exception e){
            logger.error(e.getMessage());
        }finally {
            HiveConnUtils.closeConnection(con);
        }
        return result.toString();
    }
    /**
       * @author luoyuntian
       * @date 2020-01-03 16:01
       * @description 获取hive库名
        * @param
       * @return
       */
    public static List<String> getDatabases(Connection con){
        List<String> databases = new ArrayList<>();
//        Connection con = HiveConnUtils.getConnection();
        String sql = "show databases";
        PreparedStatement ps = null;
        try{
            ps = con.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            while(rs.next()){
                databases.add((String) rs.getObject(1));
            }
            rs.close();
        }catch (Exception e){
            logger.error(e.getMessage());
        }finally {
            HiveConnUtils.closeConnection(con);
        }
        return  databases;
    }

    /**
       * @author luoyuntian
       * @date 2020-01-08 17:26
       * @description 创建库
        * @param
       * @return
       */
    public static void createDatabase(String databaseName,Connection con){
        String createDatabase = "create database "+databaseName;
        PreparedStatement ps = null;
        try{
            ps = con.prepareStatement(createDatabase);
            ps.execute();
        }catch (Exception e){
            logger.error(e.getMessage());
        }finally {
            HiveConnUtils.closeConnection(con);
        }

    }

    
    /**
       * @author luoyuntian
       * @date 2020-01-03 16:07
       * @description 根据库名获取表名
        * @param
       * @return 
       */
    public static List<String> getTables(String databasesName,Connection con){
        List<String> tables = new ArrayList<>();
//        Connection con = HiveConnUtils.getConnection();
        String selectDatabaseSql = "use "+databasesName;
        String getTables = "show tables";
        try{
            PreparedStatement ps = con.prepareStatement(selectDatabaseSql);
            ps.execute();

            PreparedStatement ps2 = con.prepareStatement(getTables);
            ResultSet rs2 = ps2.executeQuery();
            String table = null;
            while(rs2.next()){
                table = (String) rs2.getObject(1);
                tables.add(table);
            }
            rs2.close();

        }catch (Exception e){
            logger.error(e.getMessage());
        }finally {
            HiveConnUtils.closeConnection(con);
        }
        return tables;
    }
    /**
       * @author luoyuntian
       * @date 2020-01-08 10:07
       * @description 根据表结构获取存储类型
        * @param
       * @return
       */
    public static String getTableStoreType(String createDDL){
        if(createDDL.contains(HiveStoreType.FEATURE_TEXTFILE)){
            return HiveStoreType.TEXTFILE;
        }else if (createDDL.contains(HiveStoreType.FEATURE_ORCFILE)){
            return HiveStoreType.ORCFILE;
        }else if (createDDL.contains(HiveStoreType.FEATURE_PARQUET)){
            return  HiveStoreType.PARQUET;
        }else if (createDDL.contains(HiveStoreType.FEATURE_RCFILE)){
            return HiveStoreType.RCFILE;
        }else {
            return HiveStoreType.SEQUENCEFILE;
        }
    }

    /**
       * @author luoyuntian
       * @date 2020-01-08 10:41
       * @description 将查询出的建表语句格式化
        * @param
       * @return
       */
    public static String formatTableCreateDDL(String createDDL){
        //删除Location字段
        int location_index = createDDL.indexOf(HiveKeyWord.LOCATION);
        int tblproperties_index = createDDL.indexOf(HiveKeyWord.TBLPROPERTIES);
//        String result = createDDL.substring(0,location_index) + createDDL.substring(tblproperties_index);
        String result = createDDL.substring(0,location_index);
        return result;
    }

    /**
       * @author luoyuntian
       * @date 2020-01-08 14:44
       * @description 根据数据库名+建表语句建表
        * @param
       * @return
       */
    public static void  createTable(String database,String createDDL,Connection con){
//        Connection con = HiveConnUtils.getConnection();
        String changeDatabase = "use "+database;
        try {
            PreparedStatement selectDatabase =  con.prepareStatement(changeDatabase);
            selectDatabase.execute();

            PreparedStatement ps = con.prepareStatement(createDDL);
            ps.execute();
        }catch (Exception e){
            logger.error(e.getMessage());
        }finally {
            HiveConnUtils.closeConnection(con);
        }
    }
    /**
       * @author luoyuntian
       * @date 2020-01-08 14:58
       * @description 获取partions
        * @param
       * @return
       */
    public static List<String> getPartions(String database,String table,Connection con){
        List<String> partitions =  new ArrayList<>();
//        Connection con = HiveConnUtils.getConnection();
        String showPartitions = "show partitions "+database+"."+table;
        try {
            PreparedStatement ps = con.prepareStatement(showPartitions);
            ResultSet rs = ps.executeQuery();
            String partition = null;
            while(rs.next()){
                partition = (String)rs.getObject(1);
                partitions.add(partition);
            }
            System.out.println();
            rs.close();
        }catch (Exception e){
            logger.error(e.getMessage());
        }finally {
            HiveConnUtils.closeConnection(con);
        }

        return partitions;

    }
    /**
       * @author luoyuntian
       * @date 2020-01-08 15:17
       * @description 转换partition格式 eg"sex=f/class=20100501" => "sex='f',class='20100501'"
        * @param
       * @return
       */
    public static String convertPartition(String partition){
        String[] paritionLevel = partition.split("/");
        StringBuilder result = new StringBuilder();
        for(String eachLevel:paritionLevel){
            String[] oneLevel = eachLevel.split("=");
            StringBuilder oneLevelPartition = new StringBuilder(oneLevel[0]).append("=").append("'").append(oneLevel[1]).append("'");
            result.append(oneLevelPartition);
            result.append(",");
        }
        return result.deleteCharAt(result.lastIndexOf(",")).toString();

    }

    /**
       * @author luoyuntian
       * @date 2020-01-08 17:49
       * @description 为表新增分区
        * @param
       * @return
       */
    public static void addPartition(String databaseName,String tableName,String partition,Connection con){
        String addPartitonSQL = "alter table "+databaseName+"."+tableName+" add partition("+partition+")";
        PreparedStatement ps = null;
        try{
            ps = con.prepareStatement(addPartitonSQL);
            ps.execute();
        }catch (Exception e){
            logger.error(e.getMessage());
        }finally {
            HiveConnUtils.closeConnection(con);
        }

    }



    public static void main(String[] args) {


//        List<String> tables = getTables("cechealth_cechealth_wq_test");
//        System.out.println("============================================");
//        System.out.println();
//        System.out.println();
//        System.out.println("cechealth_cechealth_wq_test");
//        System.out.println();
//        System.out.println();
//        System.out.println("============================================");
//        for(String table:tables){
//            System.out.println(table+"  :  "+getHiveCreateDDL("cechealth_cechealth_wq_test"+"."+table));
//            System.out.println();
//        }

//        String str = "CREATE TABLE `hive_create_table_test_03`(\n" +
//                "  `id` string COMMENT '',\n" +
//                "  `association` string COMMENT '',\n" +
//                "  `rounds` string COMMENT '',\n" +
//                "  `election_time` string COMMENT '',\n" +
//                "  `members` string COMMENT '',\n" +
//                "  `website` string COMMENT '',\n" +
//                "  `release_time` string COMMENT '',\n" +
//                "  `update_time` string COMMENT '',\n" +
//                "  `insert_datetime` string COMMENT '',\n" +
//                "  `update_datetime` string COMMENT '')\n" +
//                "COMMENT ''\n" +
//                "PARTITIONED BY (\n" +
//                "  `miid` string COMMENT '',\n" +
//                "  `logdate` string COMMENT '')\n" +
//                "ROW FORMAT SERDE\n" +
//                "  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'\n" +
//                "STORED AS INPUTFORMAT\n" +
//                "  'org.apache.hadoop.mapred.TextInputFormat'\n" +
//                "OUTPUTFORMAT\n" +
//                "  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'\n" +
//                "LOCATION\n" +
//                " 'hdfs://mg01.cechealth.cn:8020/apps/hive/warehouse/cechealth_cechealth_wq_test.db/basic_zhyxh_info_0820_temp_001_01'\n" +
//                "TBLPROPERTIES (\n" +
//                "  'transient_lastDdlTime'='1578040612')";
//        String newTableCreateDDL = formatTableCreateDDL(str);
//        System.out.println(newTableCreateDDL);
//        hiveCreateTable("cechealth_cechealth_wq_test",newTableCreateDDL);



//        System.out.println(getPartions("cechealth_cechealth_wq_test","test_hive_user_clone_2").toString());

//        System.out.println("'");

//        System.out.println(convertPartition("sex=f/class=20100501"));
        createDatabase("test_hive_create_database_02",HiveConnUtils.getConnection());
    }
}
