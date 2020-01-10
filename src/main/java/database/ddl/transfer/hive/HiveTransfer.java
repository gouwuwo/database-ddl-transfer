package database.ddl.transfer.hive;

import database.ddl.transfer.bean.HiveDataBase;
import database.ddl.transfer.bean.HiveTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 *@ClassName HiveTransfer
 *@Description TODO
 *@Author luoyuntian
 *@Date 2020-01-08 15:26
 *@Version
 **/
public final class HiveTransfer {
    private static Logger logger = LoggerFactory.getLogger(HiveTransfer.class);
    /**
       * @author luoyuntian
       * @date 2020-01-08 16:03
       * @description 根据配置转换所有表结构
        * @param
       * @return
       */
    public static  boolean transferAll(String sourceUrl,String sourceUserName,String sourcePassword,String targetUrl,String targetUserName,String targetPassword,String driver){
        Connection sourceCon = HiveConnUtils.getHiveConnection(sourceUrl,driver,sourceUserName,sourcePassword);
        Connection targetCon = HiveConnUtils.getHiveConnection(targetUrl,driver,targetUserName,targetPassword);
        List<HiveDataBase> dataBases = getHiveDatabases(sourceCon);
        //创建库
        createDatabases(targetCon,dataBases);
        //创建表
        for(HiveDataBase dataBase:dataBases){
            List<HiveTable> tables = getHiveTables(sourceCon,dataBase);
            createTables(targetCon,tables);
            //加入分区
            for(HiveTable table:tables){
                addPartition(targetCon,table);
            }
        }
        return true;
    }
    /**
       * @author luoyuntian
       * @date 2020-01-08 16:07
       * @description 获取源表所有库
        * @param
       * @return
       */
    public static List<HiveDataBase> getHiveDatabases(Connection sourceCon){
        List<HiveDataBase> dataBases = new ArrayList<>();
        List<String> databaseNames = HiveExecutSQL.getDatabases(sourceCon);
        for(String databaseName: databaseNames){
            HiveDataBase hiveDataBase = new HiveDataBase();
            List<String> tables = HiveExecutSQL.getTables(databaseName,sourceCon);
            hiveDataBase.setDataBaseName(databaseName);
            hiveDataBase.setTables(tables);
            dataBases.add(hiveDataBase);
        }
        return dataBases;
    }
    /**
       * @author luoyuntian
       * @date 2020-01-08 16:25
       * @description 获取单个库下的所有表
        * @param
       * @return
       */
    public static List<HiveTable> getHiveTables(Connection sourceCon,HiveDataBase dataBase){
        List<HiveTable> tables = new ArrayList<>();
        List<String> tableNames = dataBase.getTables();
        String databaseName = dataBase.getDataBaseName();
        for(String tableName:tableNames){
            HiveTable table = new HiveTable();
            List<String> partitions = HiveExecutSQL.getPartions(databaseName,tableName,sourceCon);
            String creatDDL = HiveExecutSQL.getHiveCreateDDL(databaseName,tableName,sourceCon);
            table.setCreateTableDDL(creatDDL);
            table.setPartitions(partitions);
            table.setTableName(tableName);
            table.setDatabaseName(databaseName);
            tables.add(table);
        }
        return tables;
    }
    /**
       * @author luoyuntian
       * @date 2020-01-08 16:49
       * @description 在目标路径创建库
        * @param
       * @return
       */
    public static boolean createDatabases(Connection targetCon,List<HiveDataBase> dataBases){
        for(HiveDataBase dataBase:dataBases){
            HiveExecutSQL.createDatabase(dataBase.getDataBaseName(),targetCon);
        }
        return true;
    }

    /**
       * @author luoyuntian
       * @date 2020-01-08 16:50
       * @description 在目标路径创建表
        * @param
       * @return
       */
    public static boolean createTables(Connection targetCon,List<HiveTable> tables){
        for(HiveTable table:tables){
            String creatDDL =  HiveExecutSQL.formatTableCreateDDL(table.getCreateTableDDL());
            HiveExecutSQL.createTable(table.getDatabaseName(),creatDDL,targetCon);
        }
        return true;
    }
    /**
       * @author luoyuntian
       * @date 2020-01-08 17:51
       * @description 新增分区
        * @param
       * @return
       */
    public static boolean addPartition(Connection targetCon,HiveTable table){
        String databaseName = table.getDatabaseName();
        String tableName = table.getTableName();
        List<String> partitions = table.getPartitions();
        List<String> convertPartitons =  new ArrayList<>();
        for(String partition:partitions){
            String convertpartition = HiveExecutSQL.convertPartition(partition);
            convertPartitons.add(convertpartition);
            HiveExecutSQL.addPartition(databaseName,tableName,partition,targetCon);
        }
        return true;
    }
}
