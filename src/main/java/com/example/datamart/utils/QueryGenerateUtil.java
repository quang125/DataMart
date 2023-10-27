package com.example.datamart.utils;

import com.example.datamart.data.FieldKind;
import java.util.*;


/**
 * @author QuangNN
 */
public class QueryGenerateUtil {
    public static String getAllByFields(String schema, String table,String field){
        return "select distinct ("+field+ ") from " +schema +"."+ table;
    }
    public static String specificValueQuery(Map<String,List<String>>map, String mainField, String mainFieldValue, String inputField, String dataField,
                                            String inputFieldValue){
        String query="select " +dataField+" from \n(";
        for(String schema:map.keySet()){
            for(String table:map.get(schema)){
                query+="select "+dataField+" from "+schema+"."+table+" where "+inputField+"="+inputFieldValue+
                        " and "+mainField+"="+mainFieldValue+"\nUNION ";
            }
        }
        query=query.substring(0, query.length()-6);
        query += ") as sub";
        return query;
    }
    public static String queryForMaxValues(Map<String,List<String>>map,  String field){
        String query="select max("+field+") from\n(";
        for(String schema:map.keySet()){
            for(String table:map.get(schema)){
                query+="select max("+field+") as "+field+" from "+schema+"."+table+"\nUNION ";
            }
        }
        query=query.substring(0, query.length()-6);
        query += ") as sub";
        System.out.println(query);
        return  query;
    }

}
