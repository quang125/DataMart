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
    /**
     *  Query cho loại yêu cầu đếm, mà không cần biết đâu là trường đầu ra và cũng không có trường đầu vào
     *  Ví dụ: đã nạp bao nhiêu lần
     */
    public static String queryType1(String schema, String table, String mainField){
        return "select "+mainField+", count(*) from "+schema+"."+table+" where account_id in (SELECT DISTINCT account_id FROM (\n" +
                "    SELECT account_id FROM dwh_falcon_2.api_inapp_log_raw_data ailrd \n" +
                "    UNION\n" +
                "    SELECT account_id FROM dwh_falcon_2.api_inapp_log_raw_data_2023_09_11 ailrd\n" +
                "    UNION\n" +
                "    SELECT account_id FROM dwh_falcon_2.api_inapp_log_raw_data_2023_09_18 ailrd \n" +
                "    UNION\n" +
                "    SELECT account_id FROM dwh_falcon_2.api_inapp_log_raw_data_2023_09_25 ailrd \n" +
                "    UNION\n" +
                "    SELECT account_id FROM dwh_falcon_2.api_inapp_log_raw_data_2023_10_02 ailrd \n" +
                "    UNION\n" +
                "    SELECT account_id FROM dwh_falcon_2.api_inapp_log_raw_data_2023_10_09 ailrd \n" +
                "    UNION\n" +
                "    SELECT account_id FROM dwh_falcon_2.api_inapp_log_raw_data_2023_10_16 ailrd \n" +
                "    UNION\n" +
                "    SELECT account_id FROM dwh_falcon_2.api_inapp_log_raw_data_2023_10_23 ailrd \n" +
                "    UNION\n" +
                "    SELECT account_id FROM dwh_falcon_2.api_inapp_log_raw_data_2023_10_30 ailrd\n" +
                ") AS combined_results) group by "+mainField;
    }
    /**
     *  Query cho loại yêu cầu đếm, mà không có điều kiện trường đầu vào, nhưng có đặt điều kiện trên trường đầu ra
     *  Ví dụ: đã fail bao nhiêu lần (where status=fail)
     */
    public static String queryType2(String schema, String table, String mainField, String dataField, String dataFieldValue){
        return "select "+mainField+", count(*) from "+schema+"."+table+" where "+dataField+"='"+dataFieldValue+"' and account_id in (SELECT DISTINCT account_id FROM (\n" +
                "    SELECT account_id FROM dwh_falcon_2.api_inapp_log_raw_data ailrd \n" +
                "    UNION\n" +
                "    SELECT account_id FROM dwh_falcon_2.api_inapp_log_raw_data_2023_09_11 ailrd\n" +
                "    UNION\n" +
                "    SELECT account_id FROM dwh_falcon_2.api_inapp_log_raw_data_2023_09_18 ailrd \n" +
                "    UNION\n" +
                "    SELECT account_id FROM dwh_falcon_2.api_inapp_log_raw_data_2023_09_25 ailrd \n" +
                "    UNION\n" +
                "    SELECT account_id FROM dwh_falcon_2.api_inapp_log_raw_data_2023_10_02 ailrd \n" +
                "    UNION\n" +
                "    SELECT account_id FROM dwh_falcon_2.api_inapp_log_raw_data_2023_10_09 ailrd \n" +
                "    UNION\n" +
                "    SELECT account_id FROM dwh_falcon_2.api_inapp_log_raw_data_2023_10_16 ailrd \n" +
                "    UNION\n" +
                "    SELECT account_id FROM dwh_falcon_2.api_inapp_log_raw_data_2023_10_23 ailrd \n" +
                "    UNION\n" +
                "    SELECT account_id FROM dwh_falcon_2.api_inapp_log_raw_data_2023_10_30 ailrd\n" +
                ") AS combined_results) group by "+mainField;
    }
    /**
     *  Query cho loại yêu cầu tổng hợp max/min/sum/avg/..., mà không có điều kiện trường đầu vào, cũng không có đặt giá trị trên trường đầu ra
     *  Ví dụ: lần nạp nhiều nhất là bao nhiêu tiền: max(price_usd), lần nạp cuối là bao giờ: max(created_date), tổng nạp bao nhiêu tiền: sum(price_usd),...
     *  Lưu ý: loại này có thể là int, long, double, date
     */
    public static String queryType3(String schema, String table, String mainField, List<String> dataFields, List<String> types){
        StringBuilder query=new StringBuilder("select "+mainField);
        int c=0;
        for(int i=0;i<dataFields.size(); i ++){
            query.append(", ").append(types.get(i)).append("(").append(dataFields.get(i)).append(") as a").append(c);
            c++;
        }
        return query.append(" from ").append(schema).append(".").append(table).append(" where account_id in (SELECT DISTINCT account_id FROM (\n" +
                "    SELECT account_id FROM dwh_falcon_2.api_inapp_log_raw_data ailrd \n" +
                "    UNION\n" +
                "    SELECT account_id FROM dwh_falcon_2.api_inapp_log_raw_data_2023_09_11 ailrd\n" +
                "    UNION\n" +
                "    SELECT account_id FROM dwh_falcon_2.api_inapp_log_raw_data_2023_09_18 ailrd \n" +
                "    UNION\n" +
                "    SELECT account_id FROM dwh_falcon_2.api_inapp_log_raw_data_2023_09_25 ailrd \n" +
                "    UNION\n" +
                "    SELECT account_id FROM dwh_falcon_2.api_inapp_log_raw_data_2023_10_02 ailrd \n" +
                "    UNION\n" +
                "    SELECT account_id FROM dwh_falcon_2.api_inapp_log_raw_data_2023_10_09 ailrd \n" +
                "    UNION\n" +
                "    SELECT account_id FROM dwh_falcon_2.api_inapp_log_raw_data_2023_10_16 ailrd \n" +
                "    UNION\n" +
                "    SELECT account_id FROM dwh_falcon_2.api_inapp_log_raw_data_2023_10_23 ailrd \n" +
                "    UNION\n" +
                "    SELECT account_id FROM dwh_falcon_2.api_inapp_log_raw_data_2023_10_30 ailrd\n" +
                ") AS combined_results)").append(" group by ").append(mainField).toString();
    }
    /**
     *  Query cho loại yêu cầu tìm giá trị xác định, có đặt giá trị trên trường đầu ra là giá trị max/min
     *  Ví dụ: lần cuối cùng chơi bao lâu, lần cuối cùng nạp bao nhiêu, lần đầu tiên chơi bao lâu,...
     *  Lưu ý: trường này có thể là long/double
     */
    public static String queryType4(String schema, String table, String mainField, List<String> dataFields, String inputFields,String type){
        StringBuilder query=new StringBuilder("select "+mainField);
        for(String dataField : dataFields){
            query.append(", ").append(dataField);
        }
        String queryTemp=query.toString();
        return query+" from (\n  "+queryTemp+", "+"ROW_NUMBER() OVER (PARTITION BY "+mainField+" ORDER BY "+inputFields+(type.equals("max")?" desc":" asc")
        +") as row_num\n"+" from "+schema+"."+table+"\n) as ranked\n where row_num=1 and account_id in (SELECT DISTINCT account_id FROM (\n" +
                "    SELECT account_id FROM dwh_falcon_2.api_inapp_log_raw_data ailrd \n" +
                "    UNION\n" +
                "    SELECT account_id FROM dwh_falcon_2.api_inapp_log_raw_data_2023_09_11 ailrd\n" +
                "    UNION\n" +
                "    SELECT account_id FROM dwh_falcon_2.api_inapp_log_raw_data_2023_09_18 ailrd \n" +
                "    UNION\n" +
                "    SELECT account_id FROM dwh_falcon_2.api_inapp_log_raw_data_2023_09_25 ailrd \n" +
                "    UNION\n" +
                "    SELECT account_id FROM dwh_falcon_2.api_inapp_log_raw_data_2023_10_02 ailrd \n" +
                "    UNION\n" +
                "    SELECT account_id FROM dwh_falcon_2.api_inapp_log_raw_data_2023_10_09 ailrd \n" +
                "    UNION\n" +
                "    SELECT account_id FROM dwh_falcon_2.api_inapp_log_raw_data_2023_10_16 ailrd \n" +
                "    UNION\n" +
                "    SELECT account_id FROM dwh_falcon_2.api_inapp_log_raw_data_2023_10_23 ailrd \n" +
                "    UNION\n" +
                "    SELECT account_id FROM dwh_falcon_2.api_inapp_log_raw_data_2023_10_30 ailrd\n" +
                ") AS combined_results)";
    }
    /**
     *  Query cho loại yêu cầu đếm theo kiểu group by, thay vì đếm toàn bộ, sẽ chia trường điều kiện ra theo các giá trị riêng biệt rồi đếm theo điều kiện đó
     *
     */
    public static String queryType5(String schema, String table, String mainField, String inputField){
        return "select "+mainField+", "+inputField+", count(*) from "+schema+"."+table+" group by "+mainField+", "+inputField;
    }
}
