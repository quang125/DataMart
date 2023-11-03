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
    public static String queryType1(String schema, String table, String mainField, String whereIn){
        return "select "+mainField+", count(*) from "+schema+"."+table+" where account_id in ("+whereIn+
                " AS combined_results) group by "+mainField;
    }
    /**
     *  Query cho loại yêu cầu đếm, mà không có điều kiện trường đầu vào, nhưng có đặt điều kiện trên trường đầu ra
     *  Ví dụ: đã fail bao nhiêu lần (where status=fail)
     */
    public static String queryType2(String schema, String table, String mainField, String dataField, String dataFieldValue, String whereIn){
        return "select "+mainField+", count(*) from "+schema+"."+table+" where "+dataField+"='"+dataFieldValue+"' and account_id in (" +whereIn +
                " AS combined_results) group by "+mainField;
    }
    /**
     *  Query cho loại yêu cầu tổng hợp max/min/sum/avg/..., mà không có điều kiện trường đầu vào, cũng không có đặt giá trị trên trường đầu ra
     *  Ví dụ: lần nạp nhiều nhất là bao nhiêu tiền: max(price_usd), lần nạp cuối là bao giờ: max(created_date), tổng nạp bao nhiêu tiền: sum(price_usd),...
     *  Lưu ý: loại này có thể là int, long, double, date
     */
    public static String queryType3(String schema, String table, String mainField, List<String> dataFields, List<String> types, String whereIn){
        StringBuilder query=new StringBuilder("select "+mainField);
        int c=0;
        for(int i=0;i<dataFields.size(); i ++){
            query.append(", ").append(types.get(i)).append("(").append(dataFields.get(i)).append(") as a").append(c);
            c++;
        }
        return query.append(" from ").append(schema).append(".").append(table).append(" where account_id in ("+whereIn+
                " AS combined_results)").append(" group by ").append(mainField).toString();
    }
    /**
     *  Query cho loại yêu cầu tìm giá trị xác định, có đặt giá trị trên trường đầu ra là giá trị max/min
     *  Ví dụ: lần cuối cùng chơi bao lâu, lần cuối cùng nạp bao nhiêu, lần đầu tiên chơi bao lâu,...
     *  Lưu ý: trường này có thể là long/double
     */
    public static String queryType4(String schema, String table, String mainField, List<String> dataFields, String inputFields,String type, String whereIn, int order){
        StringBuilder query=new StringBuilder("select "+mainField);
        for(String dataField : dataFields){
            query.append(", ").append(dataField);
        }
        String queryTemp=query.toString();
        return query+" from (\n  "+queryTemp+", "+"ROW_NUMBER() OVER (PARTITION BY "+mainField+" ORDER BY "+inputFields+(type.equals("max")?" desc":" asc")
        +") as row_num\n"+" from "+schema+"."+table+"\n) as ranked\n where row_num="+order+ " and account_id in ("+whereIn +
                " AS combined_results)";
    }
    /**
     *  Query cho loại yêu cầu đếm theo kiểu group by, thay vì đếm toàn bộ, sẽ chia trường điều kiện ra theo các giá trị riêng biệt rồi đếm theo điều kiện đó
     *  Ví dụ: mỗi loại quảng cáo xem bao nhiêu lần
     */
    public static String queryType5(String schema, String table, String mainField, String inputField, String whereIn){
        return "select "+mainField+", "+inputField+", count(*) from "+schema+"."+table+" where "+mainField+" in("+whereIn+" as combined_results)"+" group by "+mainField+", "+inputField;
    }
    /**
     *  Query cho loại yêu cầu tìm ra các records khác biệt theo đầu vào
     *  Ví dụ: Tìm các records khác biệt theo account_id+platform (mục đích tìm ra platform mà account_id sử dụng)
     *  Tìm các records khác biệt theo account_id+country (mục đích tìm ra các country cho account_id)
     */
    public static String queryType6(String schema, String table, String mainField, String inputField, String whereIn){
        return "select "+mainField+", "+inputField+" from "+schema+"."+table+" where "+mainField+" in("+whereIn+" as combined_results)"+" group by "+mainField+", "+inputField;
    }
    /**
     *  Query cho loại yêu cầu tổng hợp theo tháng
     *  Ví dụ: tìm số lượng user lần đầu nạp trong tháng, tổng số tiền các user lần đầu đã nạp,... (not in các tháng nhỏ hơn)
     *  Tìm số lượng user chỉ nạp duy nhất trong 1 tháng (not in toàn bộ các tháng)
     */
    public static String queryMonthType1(String schema, String table,  List<String>dataFields,List<String>difMonth) {
        String query=" with cte as(select ";
        for(String field : dataFields){
            query+=field+",";
        }
        query=query.substring(0, query.length()-1);
        query+=" from "+schema+"."+table+" group by account_id ";
        if(!difMonth.isEmpty()) query+=" having min(left(created_day,7)) not in(";
        for(String prvMonth:difMonth){
            query+="'"+prvMonth+"',";
        }
        if(!difMonth.isEmpty()){
            query=query.substring(0, query.length()-1);
            query+=")";
        }
        query+=") select count(*) as b1, sum(a2) as b2,  sum(a3) as b3 from cte";
        return query;
    }

    /**
     *  Query cho loại yêu cầu tổng hợp theo tháng
     *  Ví dụ: tìm số lượng user lần đầu nạp trong tháng, tổng số tiền các user lần đầu đã nạp,... (not in các tháng nhỏ hơn)
     *  Tìm số lượng user chỉ nạp duy nhất trong 1 tháng (not in toàn bộ các tháng)
     */
    public static String queryMonthType2(String schema, String table, String month, List<String>dataFields,List<String>difMonth) {
        String query=" with cte as(select ";
        for(String field : dataFields){
            query+=field+",";
        }
        query=query.substring(0, query.length()-1);
        query+=" from "+schema+"."+table+" group by account_id ";
        query+="having max(left(created_day,7))='"+month+"'";
        if(!difMonth.isEmpty()) query+=" and min(left(created_day,7)) not in(";
        for(String prvMonth:difMonth){
            query+="'"+prvMonth+"',";
        }
        if(!difMonth.isEmpty()){
            query=query.substring(0, query.length()-1);
            query+=")";
        }
        query+=") select count(*) as b1, sum(a2) as b2,  sum(a3) as b3 from cte";
        return query;
    }
    public static String queryMonthType3(String schema, String table, String mainFields, List<String>difMonth){
        String query="select count(distinct "+mainFields+") ";
        query+="from ( ";
        query+="select "+mainFields+" from "+schema+".fact_session_time"+" where CURRENT_DATE - TO_DATE(created_day, 'YYYY-MM-DD') <=20";
        query+=") as sub\n where "+mainFields+" in (select "+mainFields+" from "+schema+"."+table+" group by "+mainFields;
        if(!difMonth.isEmpty()) query+=" having min(left(created_day,7)) not in(";
        for(String prvMonth:difMonth){
            query+="'"+prvMonth+"',";
        }
        if(!difMonth.isEmpty()){
            query=query.substring(0, query.length()-1);
            query+=")";
        }
        query+="\n)";
        return query;
    }
    public static String queryMonthType4(String schema, String table, String month, String mainFields,  List<String>difMonth){
        String query="select count(distinct "+mainFields+") ";
        query+="from ( ";
        query+="select "+mainFields+" from "+schema+".fact_session_time"+" where CURRENT_DATE - TO_DATE(created_day, 'YYYY-MM-DD') <=20";
        query+=") as sub\n where "+mainFields+" in (select "+mainFields+" from "+schema+"."+table+" group by "+mainFields;
        query+=" having max(left(created_day,7))='"+month+"'";
        if(!difMonth.isEmpty()) query+=" and min(left(created_day,7)) not in(";
        for(String prvMonth:difMonth){
            query+="'"+prvMonth+"',";
        }
        if(!difMonth.isEmpty()){
            query=query.substring(0, query.length()-1);
            query+=")";
        }
        query+="\n)";
        return query;
    }
}
