package com.sparkTutorial.sparkSql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;
import org.apache.spark.sql.catalog.*;


import org.apache.spark.sql.catalog.Database;




public class DatabaseTableList {

    private static final String AGE_MIDPOINT = "age_midpoint";
    private static final String SALARY_MIDPOINT = "salary_midpoint";
    private static final String SALARY_MIDPOINT_BUCKET = "salary_midpoint_bucket";

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);

       // SparkSession session = SparkSession.builder().appName("StackOverFlowSurvey").master("local[1]").getOrCreate();

        //SparkSession session = SparkSession.builder().appName("StackOverFlowSurvey").config("hive.metastore.client.factory.class","com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").enableHiveSupport().master("local[1]").getOrCreate();
        SparkSession session = SparkSession.builder().appName("StackOverFlowSurvey").config("hive.metastore.client.factory.class","com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").master("local[1]").enableHiveSupport().getOrCreate();

        System.out.println("=======================================");
        // System.out.println((session.catalog().listDatabases()).getClass());
        
       // Dataset<Database> listdatabases =  session.catalog().listDatabases();
        Catalog catalog = session.catalog();
        System.out.println(catalog.toString());
        
       // Database result = session.catalog().getDatabase("jjouhiu-glue-database");
        String result = catalog.currentDatabase();
        System.out.println("currentDatabase : " + result);


        Dataset<Table> listtables = catalog.listTables("jjouhiu-glue-database");
        listtables.show();
        
        // Dataset<Table> listtables = session.catalog().listTables();
        // listtables.show();
        
        System.out.println("=======================================");
        
        session.stop();
    }
}
