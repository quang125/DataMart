package com.example.datamart.service;

import com.example.datamart.data.TableInfo;
import com.example.datamart.utils.QueryGenerateUtil;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.LocalDate;
import java.util.*;

/**
 * @author QuangNN
 */
public class TaskService {
    private static final RedshiftService redshiftDC2Service = new RedshiftService("jdbc:redshift://new-dwh-cluster.cbyg0igfhhw3.us-east-1.redshift.amazonaws.com:5439/dwh_games", "quangnn", "Yvx83kfRmHt42b6kqgM5gzjG6");
    private static final Map<String, List<String>> tableList = new HashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(TaskService.class);


    public static void preRun() throws SQLException {
        List<TableInfo> tableInfoList = redshiftDC2Service.executeSelectAllTable("select * from svv_tables");
        for (TableInfo tableInfo : tableInfoList) {
            if (tableInfo.getSchemaName().startsWith("dwh_")) {
                if (!tableInfo.getSchemaName().equals("dwh_test")) continue;
                if (tableInfo.getTableName().startsWith("api_")) {
                    List<String> tables = tableList.getOrDefault(tableInfo.getSchemaName(), new ArrayList<>());
                    tables.add(tableInfo.getTableName());
                    tableList.put(tableInfo.getSchemaName(), tables);
                }
            }
        }
    }

    public static Map<String,String> doType1(String schema, String tableString, String mainField) {
        Map<String,String>result=new HashMap<>();
        for (String table : tableList.get(schema)) {
            if (!table.startsWith(tableString)) continue;
            List<List<String>>recordList=redshiftDC2Service.executeSelect(QueryGenerateUtil.queryType1(schema, table, mainField),
                    Arrays.asList(mainField, "count"));
            for(List<String>record:recordList){
                result.put(record.get(0),String.valueOf(Long.parseLong(result.getOrDefault(record.get(0),"0"))+Long.parseLong(record.get(1))));
            }
        }
        return result;
    }

    public static Map<String,String> doType2(String schema, String tableString, String mainField, String dataField, String dataFieldValue) {
        Map<String,String>result=new HashMap<>();
        for (String table : tableList.get(schema)) {
            if (!table.startsWith(tableString)) continue;
            List<List<String>>recordList=redshiftDC2Service.executeSelect(QueryGenerateUtil.queryType2(schema, table, mainField,dataField,dataFieldValue),
                    Arrays.asList(mainField, "count"));
            for(List<String>record:recordList){
                result.put(record.get(0),String.valueOf(Long.parseLong(result.getOrDefault(record.get(0),"0"))+Long.parseLong(record.get(1))));
            }
        }
        return result;
    }

    public static Map<String,String> doType3(String schema, String tableString, String mainField, String dataField, String typeData) {
        Map<String,String>result=new HashMap<>();
        for (String table : tableList.get(schema)) {
            if (!table.startsWith(tableString)) continue;
            List<List<String>>recordList=redshiftDC2Service.executeSelect(QueryGenerateUtil.queryType3(schema, table, mainField,dataField), Arrays.asList(mainField, "sum"));
            if(typeData.equals("double")) {
                for (List<String> record : recordList) {
                    result.put(record.get(0), String.valueOf(Double.parseDouble(result.getOrDefault(record.get(0), "0.0")) + Double.parseDouble(record.get(1))));
                }
            }
            else{
                for (List<String> record : recordList) {
                    result.put(record.get(0), String.valueOf(Long.parseLong(result.getOrDefault(record.get(0), "0")) + Long.parseLong(record.get(1))));
                }
            }
        }
        return result;
    }

    public static Map<String, String> doType4(String schema, String tableString, String mainField, String dataField, String typeData, String type) {
        Map<String,String>result=new HashMap<>();
        for (String table : tableList.get(schema)) {
            if (!table.startsWith(tableString)) continue;
            List<List<String>>recordList=redshiftDC2Service.executeSelect(QueryGenerateUtil.queryType4(schema, table, mainField,dataField, type),
                    Arrays.asList(mainField, type));
            if(typeData.equals("date")) {
                for (List<String> record : recordList) {
                    LocalDate current=LocalDate.parse(result.getOrDefault(record.get(0), "2020-01-01"));
                    LocalDate here=LocalDate.parse(record.get(1));
                    if(current.isBefore(here)) result.put(record.get(0), here.toString());
                }
            }
            else if(typeData.equals("long")){
                for (List<String> record : recordList) {
                    result.put(record.get(0), String.valueOf(Math.max(Long.parseLong(result.getOrDefault(record.get(0), "0")) , Long.parseLong(record.get(1)))));
                }
            }
            else{
                for (List<String> record : recordList) {
                    result.put(record.get(0), String.valueOf(Math.max(Double.parseDouble(result.getOrDefault(record.get(0), "0.0")) , Double.parseDouble(record.get(1)))));
                }
            }
        }
        return result;
    }

    public static Map<String,String> doType5(String schema, String tableString, String mainField, String dataField, String inputField, String type, String dataType) {
        Map<String,String>result=new HashMap<>();
        for (String table : tableList.get(schema)) {
            if (!table.startsWith(tableString)) continue;
            List<List<String>>recordList=redshiftDC2Service.executeSelect(QueryGenerateUtil.queryType5(schema, table, mainField,dataField,inputField, type), Arrays.asList(mainField, dataField));
            Set<String>visited=new HashSet<>();
            if(dataType.equals("long")) {
                for (List<String> record : recordList) {
                    if (visited.contains(record.get(0))) continue;
                    visited.add(record.get(0));
                    result.put(record.get(0), String.valueOf(Math.max(Long.parseLong(result.getOrDefault(record.get(0), "0")), Long.parseLong(record.get(1)))));
                }
            }
            else{
                for (List<String> record : recordList) {
                    if (visited.contains(record.get(0))) continue;
                    visited.add(record.get(0));
                    result.put(record.get(0), String.valueOf(Math.max(Double.parseDouble(result.getOrDefault(record.get(0), "0.0")) , Double.parseDouble(record.get(1)))));
                }
            }
        }
        return result;
    }
    public static Map<String,Map<String,String>> doType6(String schema, String tableString, String mainField, String inputField) {
        Map<String,Map<String,String>>result=new HashMap<>();
        Map<String,String>map=new HashMap<>();
        for (String table : tableList.get(schema)) {
            if (!table.startsWith(tableString)) continue;
            List<List<String>>recordList=redshiftDC2Service.executeSelect(QueryGenerateUtil.queryType6(schema, table, mainField, inputField),
                    Arrays.asList(mainField, inputField, "count"));
            for(List<String>record:recordList){
                map.put(record.get(0)+" "+record.get(1),String.valueOf(Long.parseLong(map.getOrDefault(record.get(0)+" "+record.get(1),"0"))
                        +Long.parseLong(record.get(2))));
            }
        }
        for(Map.Entry<String,String>m:map.entrySet()){
            String key=m.getKey();
            String value = m.getValue();
            String[]split=key.split(" ");
            if(!result.containsKey(split[0])){
                result.put(split[0], new HashMap<>());
            }
            result.get(split[0]).put(split[1],value);
        }
        return result;
    }
    static Map<String,List<String>>res=new HashMap<>();
    static int fieldNumber;
    public static void main(String[] args) throws SQLException {
        Long currentTime = System.currentTimeMillis();
        System.out.println(currentTime);
        fieldNumber=14;
        preRun();
        int c=0;
        //số lần đã nạp
        insertResult(doType1("dwh_falcon_1","api_inapp","account_id"),c++);
        //tổng đã nạp bao nhiêu tiền
        insertResult(doType3("dwh_falcon_1","api_inapp","account_id","price_usd","double"),c++);
        //ngày cuối nạp là ngày nào (created_date_str)
        insertResult(doType4("dwh_falcon_1","api_inapp","account_id","created_date_str","date","max"),c++);
        //lần cuối nạp là lúc nào (create_date)
        insertResult(doType4("dwh_falcon_1","api_inapp","account_id","created_date","long","max"),c++);
        //lần cuối nạp, thì nạp bao nhiêu tiền
        insertResult(doType5("dwh_falcon_1","api_inapp","account_id","price_usd","created_date","max","double"),c++);
        //số tiền nạp tối đa
        insertResult(doType4("dwh_falcon_1","api_inapp","account_id","price_usd","double","max"),c++);
        //số tiền nạp tối thiểu
        insertResult(doType4("dwh_falcon_1","api_inapp","account_id","price_usd","double","min"),c++);
        //số lần đã xem quảng cáo
        insertResult(doType1("dwh_falcon_1","api_ads","account_id"),c++);
        //số lần đã xem quảng cáo cho từng loại quảng cáo
//        Map<String,Map<String,String>>map=doType6("dwh_test","api_ads","account_id","ad_where");
//        fieldNumber+= map.size();
//        for(Map.Entry<String,Map<String,String>>m:map.entrySet()){
//            insertResult(m.getValue(),c++);
//        }
        //level cao nhất
        insertResult(doType4("dwh_falcon_1","api_level","account_id","level","long","max"),c++);
        //số lần đã fail
        insertResult(doType2("dwh_falcon_1","api_level","account_id","status","fail"),c++);
        //lần cuối đăng nhập
        insertResult(doType4("dwh_falcon_1","api_retention","account_id","created_date","long","max"),c++);
        //lần đầu đăng nhập
        insertResult(doType4("dwh_falcon_1","api_retention","account_id","created_date","long","min"),c++);
        //lần cuối chơi bao nhiêu thời gian
        insertResult(doType5("dwh_falcon_1","api_session","account_id","session_time","created_date","max","long"),c++);
        //tổng đã chơi bao nhiêu thời gian
        insertResult(doType3("dwh_falcon_1","api_session","account_id","session_time","long"),c);
        long endTime = System.currentTimeMillis();
        long executeTime=endTime - currentTime;
        System.out.println(executeTime);
    }
    public static void insertResult(Map<String,String>m, int index){
        for(Map.Entry<String,String> e:m.entrySet()){
            if(!res.containsKey(e.getKey())){
                List<String> l = new ArrayList<>(Collections.nCopies(fieldNumber, "null"));
                l.set(index,e.getValue());
                res.put(e.getKey(),l);
            }
            else{
                res.get(e.getKey()).set(index,e.getValue());
            }
        }
    }

}
