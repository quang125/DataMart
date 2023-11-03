package com.example.datamart.service;

import com.example.datamart.data.TableInfo;
import com.example.datamart.repository.MongoRepository;
import com.example.datamart.utils.QueryGenerateUtil;
import jakarta.annotation.PostConstruct;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.FileWriter;
import java.io.IOException;
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
    static Map<String, List<String>> res = new HashMap<>();
    static int fieldNumber;
    static int index = 0;
    List<String> fieldNames = new ArrayList<>(Arrays.asList("account_id", "inapp_count", "inapp_max", "inapp_last", "inapp_min", "inapp_sum","inapp_first", "inapp_last_price",
            "inapp_last_name","inapp_last_level","inapp_last_country","inapp_first_price","inapp_first_name","inapp_first_level", "inapp_first_country",
            "inapp_2nd_first_price","inapp_2nd_first_name","inapp_2nd_first_level", "inapp_2nd_first_country",
            "ads_count", "level_count_fail", "retention_last_login"
            , "retention_first_login", "session_last_play", "session_sum_play","platform"));
    @Autowired
    private MongoRepository mongoRepository;
    private static final String mainField="account_id";
    private static final String mainTable="api_inapp_log_raw_data";
    private static String whereIn="select distinct "+mainField+" from (\n";

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
                if(tableInfo.getTableName().startsWith(mainTable)){
                    whereIn+=" select "+mainField+" from "+tableInfo.getSchemaName()+"."+tableInfo.getTableName()+"\nunion\n";
                }
            }
        }
        whereIn=whereIn.substring(0,whereIn.length()-6);
        whereIn+=")";
    }

    public static Map<String, String> doType1(String schema, String tableString, String mainField) {
        Map<String, String> result = new HashMap<>();
        for (String table : tableList.get(schema)) {
            if (!table.startsWith(tableString)) continue;
            List<List<String>> recordList = redshiftDC2Service.executeSelect(QueryGenerateUtil.queryType1(schema, table, mainField, whereIn),
                    Arrays.asList(mainField, "count"));
            for (List<String> record : recordList) {
                result.put(record.get(0), String.valueOf(Long.parseLong(result.getOrDefault(record.get(0), "0")) + Long.parseLong(record.get(1))));
            }
        }
        return result;
    }

    public static Map<String, String> doType2(String schema, String tableString, String mainField, String inputField, String inputFieldValue) {
        Map<String, String> result = new HashMap<>();
        for (String table : tableList.get(schema)) {
            if (!table.startsWith(tableString)) continue;
            List<List<String>> recordList = redshiftDC2Service.executeSelect(QueryGenerateUtil.queryType2(schema, table, mainField, inputField, inputFieldValue,whereIn),
                    Arrays.asList(mainField, "count"));
            for (List<String> record : recordList) {
                result.put(record.get(0), String.valueOf(Long.parseLong(result.getOrDefault(record.get(0), "0")) + Long.parseLong(record.get(1))));
            }
        }
        return result;
    }

    public static Map<String, LinkedHashMap<String, String>> doType3(String schema, String tableString, String mainField, List<String> dataFields, List<String> types, List<String> dataTypes) {
        Map<String, LinkedHashMap<String, String>> result = new HashMap<>();
        for (String table : tableList.get(schema)) {
            if (!table.startsWith(tableString)) continue;
            List<String> fields = new ArrayList<>();
            int c = 0;
            for (int k = 0; k < types.size(); k++) {
                fields.add("a" + c);
                c++;
            }
            fields.add(mainField);
            List<List<String>> recordList = redshiftDC2Service.executeSelect(QueryGenerateUtil.queryType3(schema, table, mainField, dataFields, types,whereIn), fields);
            for (List<String> strings : recordList) {
                if (!result.containsKey(strings.get(types.size())))
                    result.put(strings.get(types.size()), new LinkedHashMap<>());
                LinkedHashMap<String, String> fieldsResult = new LinkedHashMap<>(result.get(strings.get(types.size())));
                for (int j = 0; j < types.size(); j++) {
                    if (types.get(j).equals("sum")) {
                        if (dataTypes.get(j).equals("double"))
                            fieldsResult.put(dataFields.get(j) + "_sum", String.valueOf(Double.parseDouble(fieldsResult.getOrDefault(dataFields.get(j) + "_sum", "0.0")) + Double.parseDouble(strings.get(j))));
                        if (dataTypes.get(j).equals("long"))
                            fieldsResult.put(dataFields.get(j) + "_sum", String.valueOf(Long.parseLong(fieldsResult.getOrDefault(dataFields.get(j) + "_sum", "0")) + Long.parseLong(strings.get(j))));
                    }
                    if (types.get(j).equals("max")) {
                        if (dataTypes.get(j).equals("double"))
                            fieldsResult.put(dataFields.get(j) + "_max", String.valueOf(Math.max(Double.parseDouble(fieldsResult.getOrDefault(dataFields.get(j) + "_max", "0.0")), Double.parseDouble(strings.get(j)))));
                        else if (dataTypes.get(j).equals("long"))
                            fieldsResult.put(dataFields.get(j) + "_max", String.valueOf(Math.max(Long.parseLong(fieldsResult.getOrDefault(dataFields.get(j) + "_max", "0")), Long.parseLong(strings.get(j)))));
                        else {
                            LocalDate currentDate = LocalDate.parse(fieldsResult.getOrDefault(dataFields.get(j) + "_max", "2023-01-01"));
                            if (LocalDate.parse(strings.get(j)).isAfter(currentDate)) {
                                fieldsResult.put(dataFields.get(j) + "_max", strings.get(j));
                            }
                        }
                    }
                    if (types.get(j).equals("min")) {
                        if (dataTypes.get(j).equals("double"))
                            fieldsResult.put(dataFields.get(j) + "_min", String.valueOf(Math.min(Double.parseDouble(fieldsResult.getOrDefault(dataFields.get(j) + "_min", "1000000000.0")), Double.parseDouble(strings.get(j)))));
                        else if (dataTypes.get(j).equals("long"))
                            fieldsResult.put(dataFields.get(j) + "_min", String.valueOf(Math.min(Long.parseLong(fieldsResult.getOrDefault(dataFields.get(j) + "_min", "1000000000000")), Long.parseLong(strings.get(j)))));
                        else {
                            LocalDate currentDate = LocalDate.parse(fieldsResult.getOrDefault(dataFields.get(j) + "_min", "2070-01-01"));
                            if (LocalDate.parse(strings.get(j)).isBefore(currentDate)) {
                                fieldsResult.put(dataFields.get(j) + "_min", strings.get(j));
                            }
                        }
                    }
                }
                result.put(strings.get(types.size()), fieldsResult);
            }
        }
        return result;
    }

    public static Map<String, LinkedHashMap<String, String>> doType4(String schema, String tableString, String mainField, List<String> dataFields, String inputField, String type, int order) {
        Map<String, LinkedHashMap<String, String>> result = new HashMap<>();
        List<String> sortedTable = new ArrayList<>();
        for (String table : tableList.get(schema)) {
            if (table.startsWith(tableString)) sortedTable.add(table);
        }
        if(type.equals("max"))sortedTable.sort((a, b) -> (a.length() == b.length() ? b.compareTo(a) : a.length() - b.length()));
        else sortedTable.sort((a, b) -> (a.length() == b.length() ? a.compareTo(b) : b.length() - a.length()));
        for (String table : sortedTable) {
            List<String> fields = new ArrayList<>(dataFields);
            fields.add(mainField);
            List<List<String>> recordList = redshiftDC2Service.executeSelect(QueryGenerateUtil.queryType4(schema, table, mainField, dataFields, inputField, type,whereIn,order), fields);
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

    public static Map<String, LinkedHashMap<String, String>> doType5(String schema, String tableString, String mainField, String inputField, List<String> distinctValues) {
        Map<String, LinkedHashMap<String, String>> result = new HashMap<>();
        for (String table : tableList.get(schema)) {
            if (!table.startsWith(tableString)) continue;
            List<List<String>>recordList=redshiftDC2Service.executeSelect(QueryGenerateUtil.queryType5(schema, table, mainField, inputField,whereIn),
                    Arrays.asList(mainField, inputField, "count"));
            for(List<String>record:recordList){
                if(!result.containsKey(record.get(0))){
                    LinkedHashMap<String,String>map=new LinkedHashMap<>();
                    for(String value:distinctValues) map.put(value,"0");
                    result.put(record.get(0),map);
                }
                LinkedHashMap<String,String>m=result.get(record.get(0));
                m.put(record.get(1), String.valueOf(Long.parseLong(m.get(record.get(1)))+Long.parseLong(record.get(2))));
                result.put(record.get(0),m);
            }
        }
        return result;
    }
    public static Map<String, String> doType6(String schema, String tableString, String mainField, String inputField) {
        Map<String, String> result = new HashMap<>();
        for (String table : tableList.get(schema)) {
            if (!table.startsWith(tableString)) continue;
            List<List<String>> recordList = redshiftDC2Service.executeSelect(QueryGenerateUtil.queryType6(schema, table, mainField, inputField,whereIn),
                    Arrays.asList(mainField, inputField));
            for (List<String> record : recordList) {
                result.put(record.get(0), record.get(1));
            }
        }
        return result;
    }

    public static void insertOne(Map<String, String> m) {
        for (Map.Entry<String, String> e : m.entrySet()) {
            if (!res.containsKey(e.getKey())) {
                List<String> l = new ArrayList<>(Collections.nCopies(fieldNumber, "null"));
                l.set(index, e.getValue());
                res.put(e.getKey(), l);
            } else {
                res.get(e.getKey()).set(index, e.getValue());
            }
        }
        index += 1;
    }

    public static void insertMul(Map<String, LinkedHashMap<String, String>> m) {
        int newInd = index;
        for (Map.Entry<String, LinkedHashMap<String, String>> e : m.entrySet()) {
            int z = index;
            if (!res.containsKey(e.getKey())) {
                List<String> l = new ArrayList<>(Collections.nCopies(fieldNumber, "null"));
                for (Map.Entry<String, String> x : e.getValue().entrySet()) {
                    l.set(z, x.getValue());
                    z++;
                }
                res.put(e.getKey(), l);
            } else {
                for (Map.Entry<String, String> x : e.getValue().entrySet()) {
                    res.get(e.getKey()).set(z, x.getValue());
                    z++;
                }
            }
            if (newInd == index) newInd += z - index;
        }
        index = newInd;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
//        Long currentTime = System.currentTimeMillis();
//        logger.info(String.valueOf(currentTime));
//        fieldNumber = 25;
//        //số lần đã nạp
//        insertOne(doType1("dwh_falcon_2", "api_inapp", "account_id"));
//        //lần nạp tối đa, lần nạp cuối, lần nạp tối thiểu, tổng nạp bao nhiêu tiền, lần nạp đầu
//        insertMul(doType3("dwh_falcon_2", "api_inapp_log_raw_data", "account_id", Arrays.asList("price_usd", "created_date_str", "price_usd", "price_usd", "created_date_str"),
//                Arrays.asList("max", "max", "min", "sum","min"), Arrays.asList("double", "date", "double", "double","date")));
//        //lần nạp cuối: nạp bao nhiêu tiền, tên gói nạp, level chơi khi nạp, đất nước
//        insertMul(doType4("dwh_falcon_2", "api_inapp_log_raw_data", "account_id", List.of("price_usd","product_id","level","country"),
//                "created_date", "max",1));
//       // lần nạp đầu: nạp bao nhiêu tiền, tên gói nạp, level chơi khi nạp, đất nước
//        insertMul(doType4("dwh_falcon_2", "api_inapp_log_raw_data", "account_id", List.of("price_usd","product_id","level","country"),
//                "created_date", "min",1));
//        //lần nạp thứ 2: nạp bao nhiêu tiền, tên gói nạp, level chơi khi nạp, đất nước
//        insertMul(doType4("dwh_falcon_2", "api_inapp_log_raw_data", "account_id", List.of("price_usd","product_id","level","country"),
//                "created_date", "min",2));
//        //xem bao nhiêu quảng cáo
//        insertOne(doType1("dwh_falcon_2", "api_ads_log_raw_data", "account_id"));
//        //fail bao nhiêu lần
//        insertOne(doType2("dwh_falcon_2", "api_level_log_raw_data", "account_id", "status", "fail"));
//        //lần đăng nhập cuối, lần đầu đăng nhập
//        insertMul(doType3("dwh_falcon_2", "api_resource_log_raw_data", "account_id", Arrays.asList("created_date_str", "created_date_str"),
//                Arrays.asList("max", "min"), Arrays.asList("date", "date")));
//        //lần chơi cuối chơi bao lâu
//        insertMul(doType4("dwh_falcon_2", "api_session_raw_data", "account_id", List.of("session_time"),
//                "created_date", "max",1));
//        //tổng thời gian đã chơi
//        insertMul(doType3("dwh_falcon_2", "api_session_raw_data", "account_id", List.of("session_time"),
//                List.of("sum"), List.of("long")));
//        //thuộc platform nào
//        insertOne(doType6("dwh_falcon_2","api_inapp_log_raw_data","account_id","platform"));
//        writeCsv();
//        long endTime = System.currentTimeMillis();
//        long executeTime = endTime - currentTime;
//        logger.info(String.valueOf(executeTime));
    }

    public void writeCsv() {
        try (CSVPrinter csvPrinter = new CSVPrinter(new FileWriter("C://csv//test8.csv"), CSVFormat.DEFAULT)) {
            csvPrinter.printRecord(fieldNames);
            for (Map.Entry<String, List<String>> e : res.entrySet()) {
                List<String> record = new ArrayList<>();
                record.add(e.getKey());
                for (int i = 1; i < fieldNames.size(); i++) {
                    record.add(e.getValue().get(i-1));
                }
                csvPrinter.printRecord(record);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
