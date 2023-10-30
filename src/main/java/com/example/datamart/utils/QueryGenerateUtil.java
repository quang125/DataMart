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
        return "select "+mainField+", count(*) from "+schema+"."+table+" group by "+mainField;
    }
    /**
     *  Query cho loại yêu cầu đếm, mà không có điều kiện trường đầu vào, nhưng có đặt điều kiện trên trường đầu ra
     *  Ví dụ: đã fail bao nhiêu lần (where status=fail)
     */
    public static String queryType2(String schema, String table, String mainField, String dataField, String dataFieldValue){
        return "select "+mainField+", count(*) from "+schema+"."+table+" where "+dataField+"='"+dataFieldValue+"' group by "+mainField;
    }
    /**
     *  Query cho loại yêu cầu tìm tổng,  không có điều kiện trường đầu vào, có trường đầu ra nhưng không có đặt điều kiện trên trường đầu ra
     *  Ví dụ: tổng cộng đã nạp bao nhiêu tiền
     *  Lưu ý: tổng này có thể là int/long/double
     */
    public static String queryType3(String schema, String table, String mainField, String dataField){
        return "select "+mainField+", sum("+dataField+") from "+schema+"."+table+" group by "+mainField;
    }
    /**
     *  Query cho loại yêu cầu tìm max/min, mà không có điều kiện trường đầu vào, cũng không có đặt giá trị trên trường đầu ra
     *  Ví dụ: lần nạp nhiều nhất là bao nhiêu tiền, lần nạp cuối là bao giờ, lần nạp cuối là bao giờ
     *  Lưu ý: loại này có thể là int, long, double, date
     */
    public static String queryType4(String schema, String table, String mainField, String dataField, String type){
        return "select "+mainField+", "+type+"("+dataField+") from "+schema+"."+table+" group by "+mainField;
    }
    /**
     *  Query cho loại yêu cầu tìm giá trị xác định, có đặt giá trị trên trường đầu ra là giá trị max/min
     *  Ví dụ: lần cuối cùng chơi bao lâu, lần cuối cùng nạp bao nhiêu, lần đầu tiên chơi bao lâu,...
     *  Lưu ý: trường này có thể là long/double
     */
    public static String queryType5(String schema, String table, String mainField, String dataField, String inputFields,String type){
        /*
        SELECT account_id, session_time
FROM (
    SELECT
        account_id,
        session_time,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY created_date DESC) AS row_num
    FROM dwh_test.api_session_raw_data
) AS ranked
WHERE row_num = 1;

         */
        return "select "+mainField+", "+dataField+" from (\n select "+mainField+", "+dataField+", "+"ROW_NUMBER() OVER (PARTITION BY "+mainField+" ORDER BY "+inputFields+(type.equals("max")?" desc":" asc")
        +") as row_num\n"+" from "+schema+"."+table+"\n) as ranked\n where row_num=1";
    }
    /**
     *  Query cho loại yêu cầu đếm theo kiểu group by, thay vì đếm toàn bộ, sẽ chia trường điều kiện ra theo các giá trị riêng biệt rồi đếm theo điều kiện đó
     *  Ví dụ: lần cuối cùng chơi bao lâu, lần cuối cùng nạp bao nhiêu,...
     */
    public static String queryType6(String schema, String table, String mainField, String inputField){
        return "select "+mainField+", "+inputField+", count(*) from "+schema+"."+table+" group by "+mainField+", "+inputField;
    }
}
