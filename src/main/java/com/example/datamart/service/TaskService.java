package com.example.datamart.service;

import com.example.datamart.data.TableInfo;
import com.example.datamart.repository.MongoRepository;
import com.example.datamart.utils.QueryGenerateUtil;
import com.mongodb.client.MongoCollection;
import jakarta.annotation.PostConstruct;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.time.LocalDate;
import java.util.*;

/**
 * @author QuangNN
 */
@Service
public class TaskService implements InitializingBean {
    private static final RedshiftService redshiftDC2Service = new RedshiftService("jdbc:redshift://new-dwh-cluster.cbyg0igfhhw3.us-east-1.redshift.amazonaws.com:5439/dwh_games", "quangnn", "Yvx83kfRmHt42b6kqgM5gzjG6");
    private static final Map<String, List<String>> tableList = new HashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(TaskService.class);
    private static Set<String>accountId=new HashSet<>();
    @Autowired
    private  MongoRepository mongoRepository;
    @PostConstruct
    public static void preRun() throws SQLException {
        List<TableInfo> tableInfoList = redshiftDC2Service.executeSelectAllTable("select * from svv_tables");
        for (TableInfo tableInfo : tableInfoList) {
            if (tableInfo.getSchemaName().startsWith("dwh_")) {
                if (!tableInfo.getSchemaName().equals("dwh_falcon_2")) continue;
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

    public static Map<String,LinkedHashMap<String,String>> doType3(String schema, String tableString, String mainField, List<String> dataFields, List<String> types, List<String>dataTypes) {
        Map<String,LinkedHashMap<String,String>>result=new HashMap<>();
        for (String table : tableList.get(schema)) {
            if (!table.startsWith(tableString)) continue;
            List<String>fields=new ArrayList<>();
            int c=0;
            for(int k=0;k<types.size();k++){
                fields.add("a"+c);
                c++;
            }
            fields.add(mainField);
            List<List<String>>recordList=redshiftDC2Service.executeSelect(QueryGenerateUtil.queryType3(schema, table, mainField,dataFields,types), fields);
            for(int i=0;i<recordList.size();i++){
                if(!result.containsKey(recordList.get(i).get(types.size()))) result.put(recordList.get(i).get(types.size()),new LinkedHashMap<>());
                LinkedHashMap<String,String>fieldsResult=new LinkedHashMap<>(result.get(recordList.get(i).get(types.size())));
                for(int j=0;j<types.size();j++){
                    if(types.get(j).equals("sum")) {
                        if(dataTypes.get(j).equals("double")) fieldsResult.put(dataFields.get(j)+"_sum",String.valueOf(Double.parseDouble(fieldsResult.getOrDefault(dataFields.get(j)+"_sum","0.0"))+Double.parseDouble(recordList.get(i).get(j))));
                        if(dataTypes.get(j).equals("long")) fieldsResult.put(dataFields.get(j)+"_sum",String.valueOf(Long.parseLong(fieldsResult.getOrDefault(dataFields.get(j)+"_sum","0"))+Long.parseLong(recordList.get(i).get(j))));
                    }
                    if(types.get(j).equals("max")) {
                        if(dataTypes.get(j).equals("double")) fieldsResult.put(dataFields.get(j)+"_max",String.valueOf(Math.max(Double.parseDouble(fieldsResult.getOrDefault(dataFields.get(j)+"_max","0.0")),Double.parseDouble(recordList.get(i).get(j)))));
                        else if(dataTypes.get(j).equals("long")) fieldsResult.put(dataFields.get(j)+"_max",String.valueOf(Math.max(Long.parseLong(fieldsResult.getOrDefault(dataFields.get(j)+"_max","0")),Long.parseLong(recordList.get(i).get(j)))));
                    }
                    if(types.get(j).equals("min")) {
                        if(dataTypes.get(j).equals("double")) fieldsResult.put(dataFields.get(j)+"_min",String.valueOf(Math.min(Double.parseDouble(fieldsResult.getOrDefault(dataFields.get(j)+"_min","1000000000.0")),Double.parseDouble(recordList.get(i).get(j)))));
                        else if(dataTypes.get(j).equals("long")) fieldsResult.put(dataFields.get(j)+"_min",String.valueOf(Math.min(Long.parseLong(fieldsResult.getOrDefault(dataFields.get(j)+"_min","1000000000000")),Long.parseLong(recordList.get(i).get(j)))));
                    }
                }
                result.put(recordList.get(i).get(types.size()),fieldsResult);
            }
        }
        return result;
    }
    public static Map<String,LinkedHashMap<String,String>> doType4(String schema, String tableString, String mainField, List<String> dataFields, String inputField, String type) {
        Map<String,LinkedHashMap<String,String>>result=new HashMap<>();
        List<String>sortedTable=new ArrayList<>();
        for(String table:tableList.get(schema)){
            if(table.startsWith(tableString)) sortedTable.add(table);
        }
        sortedTable.sort((a,b)->(a.length()==b.length()?b.compareTo(a):a.length()-b.length()));
        for (String table : sortedTable) {
            List<String>fields=new ArrayList<>(dataFields);
            fields.add(mainField);
            List<List<String>>recordList=redshiftDC2Service.executeSelect(QueryGenerateUtil.queryType4(schema, table, mainField,dataFields,inputField, type), fields);
            for (List<String> strings : recordList) {
                if (result.containsKey(strings.get(dataFields.size()))) continue;
                LinkedHashMap<String, String> fieldsResult = new LinkedHashMap<>();
                for (int j = 0; j < dataFields.size(); j++) {
                    fieldsResult.put(dataFields.get(j), strings.get(j));
                }
                result.put(strings.get(dataFields.size()), fieldsResult);
            }
        }
        return result;
    }
//    public static Map<String,Map<String,String>> doType6(String schema, String tableString, String mainField, String inputField) {
//        Map<String,Map<String,String>>result=new HashMap<>();
//        Map<String,String>map=new HashMap<>();
//        for (String table : tableList.get(schema)) {
//            if (!table.startsWith(tableString)) continue;
//            List<List<String>>recordList=redshiftDC2Service.executeSelect(QueryGenerateUtil.queryType6(schema, table, mainField, inputField),
//                    Arrays.asList(mainField, inputField, "count"));
//            for(List<String>record:recordList){
//                map.put(record.get(0)+" "+record.get(1),String.valueOf(Long.parseLong(map.getOrDefault(record.get(0)+" "+record.get(1),"0"))
//                        +Long.parseLong(record.get(2))));
//            }
//        }
//        for(Map.Entry<String,String>m:map.entrySet()){
//            String key=m.getKey();
//            String value = m.getValue();
//            String[]split=key.split(" ");
//            if(!result.containsKey(split[0])){
//                result.put(split[0], new HashMap<>());
//            }
//            result.get(split[0]).put(split[1],value);
//        }
//        if(tableString.startsWith("api_inapp")){
//            accountId.addAll(result.keySet());
//        }
//        return result;
//    }
    static Map<String,List<String>>res=new HashMap<>();
    static int fieldNumber;
    static int index=0;
    public static void insertOne(Map<String,String>m){
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
        index+=1;
    }
    public static void insertMul(Map<String,LinkedHashMap<String,String>>m){
        int newInd=index;
        for(Map.Entry<String,LinkedHashMap<String,String>> e:m.entrySet()){
            int z=index;
            if(!res.containsKey(e.getKey())){
                List<String> l = new ArrayList<>(Collections.nCopies(fieldNumber, "null"));
                for(Map.Entry<String,String> x:e.getValue().entrySet()){
                    l.set(z,x.getValue());
                    z++;
                }
                res.put(e.getKey(), l);
            }
            else{
                for(Map.Entry<String,String> x:e.getValue().entrySet()){
                    res.get(e.getKey()).set(z,x.getValue());
                    z++;
                }
            }
            if(newInd==index) newInd+=z-index;
        }
        index=newInd;
    }
    @Override
    public void afterPropertiesSet() throws Exception {
        Long currentTime = System.currentTimeMillis();
        System.out.println(currentTime);
        fieldNumber=13;
        //số lần đã nạp
        insertOne(doType1("dwh_falcon_2","api_inapp","account_id"));
        //lần nạp tối đa, lần nạp cuối, lần nạp tối thiểu, tổng nạp bao nhiêu tiền
        insertMul(doType3("dwh_falcon_2","api_inapp_log_raw_data","account_id",Arrays.asList("price_usd","created_date","price_usd","price_usd"),
                Arrays.asList("max","max","min","sum"),Arrays.asList("double","long","double","double")));
        //lần nạp cuối nạp bao nhiêu tiền
        insertMul(doType4("dwh_falcon_2","api_inapp_log_raw_data","account_id",Arrays.asList("price_usd"),
                "created_date","max"));
        //xem bao nhiêu quảng cáo
        insertOne(doType1("dwh_falcon_2","api_ads_log_raw_data","account_id"));
        //fail bao lần
        insertOne(doType2("dwh_falcon_2","api_level_log_raw_data","account_id","status","fail"));
        //lần đăng nhập cuối, lần đầu đăng nhập
        insertMul(doType3("dwh_falcon_2","api_retention_raw_data","account_id",Arrays.asList("created_date","created_date"),
                Arrays.asList("max","min"),Arrays.asList("long","long")));
        //lần cuối chơi bao lâu
        insertMul(doType4("dwh_falcon_2","api_session_raw_data","account_id",Arrays.asList("session_time"),
                "created_date","max"));
        //tổng thời gian đã chơi
        insertMul(doType3("dwh_falcon_2","api_session_raw_data","account_id",Arrays.asList("session_time"),
                Arrays.asList("sum"),Arrays.asList("long")));
        for(String x:res.keySet()){
            Document document = new Document();
            document.append("account_id",x);
            document.append("inapp_count",res.get(x).get(0));
            document.append("inapp_max",res.get(x).get(1));
            document.append("inapp_last",res.get(x).get(2));
            document.append("inapp_min",res.get(x).get(3));
            document.append("inapp_sum",res.get(x).get(4));
            document.append("inapp_last_price",res.get(x).get(5));
            document.append("ads_count",res.get(x).get(6));
            document.append("level_count_fail",res.get(x).get(7));
            document.append("retention_last_login",res.get(x).get(8));
            document.append("retention_first_login",res.get(x).get(9));
            document.append("session_last_play",res.get(x).get(10));
            document.append("session_sum_play",res.get(x).get(11));
            mongoRepository.saveDocument(document,"collectiontest");
            //documents.add(document);
        }
        long endTime = System.currentTimeMillis();
        long executeTime=endTime - currentTime;
        System.out.println(executeTime);
    }
}
