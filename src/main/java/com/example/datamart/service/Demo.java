package com.example.datamart.service;

import com.example.datamart.data.FieldKind;
import com.example.datamart.data.FieldTask;
import com.example.datamart.data.TableInfo;
import com.example.datamart.repository.MongoRepository;
import com.example.datamart.utils.QueryGenerateUtil;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.*;

/**
 * @author QuangNN
 */
@Service
public class Demo {
    private static Set<String> schemaList=new HashSet<>();
    private static Map<String, List<String>> tableList = new HashMap<>();
    public static void preRun() throws SQLException {
        List<TableInfo> tableInfoList = redshiftDC2Service.executeSelectAllTable("select * from svv_tables");
        for(TableInfo tableInfo:tableInfoList){
            if(tableInfo.getSchemaName().startsWith("dwh_")){
                if(!tableInfo.getSchemaName().equals("dwh_test")) continue;
                schemaList.add(tableInfo.getSchemaName());
                if(tableInfo.getTableName().startsWith("api_")){
                    List<String>tables=tableList.getOrDefault(tableInfo.getSchemaName(),new ArrayList<>());
                    tables.add(tableInfo.getTableName());
                    tableList.put(tableInfo.getSchemaName(),tables);
                }
            }
        }
    }
//    @PostConstruct
//    public static void preTask(Task task) throws SQLException {
//        System.out.println("cccc");
//        //lấy tất cả distinct main field ra
//        Set<String>mainFieldData=new HashSet<>();
//        for(String schema:schemaList){
//            for(String table:tableList.get(schema)){
//                mainFieldData.addAll(redshiftDC2Service.executeSelectDistinct(QueryGenerateUtil.getAllByFields(schema, table,task.getMainField())));
//            }
//        }
//        System.out.println(mainFieldData.size());
//        Map<String,String>res=new HashMap<>();
//        for(FieldTask fieldTask: task.getFieldTask()){
//            //các task có bảng, lấy tất cả các bảng có format tương tự của từng schema
//            Map<String,List<String>>map=new HashMap<>();
//            for(String schema:schemaList){
//                for(String table:tableList.get(schema)){
//                    System.out.println(table+" "+fieldTask.getTable());
//                    if(table.startsWith(fieldTask.getTable())){
//                        List<String>tables=map.getOrDefault(schema,new ArrayList<>());
//                        tables.add(table);
//                        map.put(schema,tables);
//                    }
//                }
//            }
//            //tìm dataFile ra
//            String data="";
//            if(fieldTask.getDataField().getKind().equals("max")||fieldTask.getDataField().getKind().equals("min")||
//                    fieldTask.getDataField().getKind().equals("sum")) data=fieldTask.getDataField().getKind()+"("+fieldTask.getDataField().getField()+")";
//            else if(fieldTask.getDataField().getKind().equals("count")) data="count(*)";
//            else data=fieldTask.getDataField().getField();
//            if(fieldTask.getInputField().getKind().equals("max")){
//                //tìm max
//                String max=redshiftDC2Service.executeCountSelect(QueryGenerateUtil.queryForMaxValues(map,fieldTask.getInputField().getField()));
//                for(String mainId:mainFieldData) {
//                    String q = QueryGenerateUtil.specificValueQuery(map, task.getMainField(), mainId, fieldTask.getInputField().getField(), data, max);
//                    System.out.println(q);
//                }
//            }
//            else if(fieldTask.getInputField().getKind().equals("group by")){
//                //vì là group by nên ta sẽ lấy hết ra các giá trị khác biệt
//                Set<String>vals=new HashSet<>();
//                for(String schema:schemaList){
//                    for(String table:tableList.get(schema)){
//                        if(table.startsWith(fieldTask.getTable())){
//                            vals.addAll(redshiftDC2Service.executeSelectDistinct(
//                                    QueryGenerateUtil.getAllByFields(schema,table,fieldTask.getInputField().getField())));
//                        }
//                    }
//                }
//                for(String mainId:mainFieldData){
//                    for(String val:vals){
//                        String q=QueryGenerateUtil.specificValueQuery(map,task.getMainField(),mainId,fieldTask.getInputField().getField(),data,val);
//                        //System.out.println(q);
//                    }
//                }
//                //System.out.println();
//            }
//            else if(fieldTask.getInputField().getKind().equals("null")){
//                for(String mainId:mainFieldData){
//                    String q = QueryGenerateUtil.specificValueQuery(map, task.getMainField(), mainId, "1",
//                            data, "1");
//                    System.out.println(q);
//                }
//                System.out.println();
//            }
//            else{
//                for(String mainId:mainFieldData) {
//                    String q = QueryGenerateUtil.specificValueQuery(map, task.getMainField(), mainId, fieldTask.getInputField().getField(),
//                            data, fieldTask.getInputField().getKind());
//                    System.out.println(q);
//                }
//                System.out.println();
//            }
//        }
//    }
//
//    public static void main(String[] args) throws SQLException {
//        List<FieldKind>fieldInputKinds = new ArrayList<>();
//        List<FieldKind>fieldDataKinds = new ArrayList<>();
//        List<FieldTask>fieldTasks=new ArrayList<>();
//        //bao nhiêu lần nap
//        fieldInputKinds.add(new FieldKind("null", "null"));
//        fieldDataKinds.add(new FieldKind("game_id", "count"));
//        //đã nạp bao nhiêu tiền
//        fieldInputKinds.add(new FieldKind("null", "null"));
//        fieldDataKinds.add(new FieldKind("price", "sum"));
//        //lần cuối nạp là lúc nào?
//        fieldInputKinds.add(new FieldKind("null", "null"));
//        fieldDataKinds.add(new FieldKind("created_date", "max"));
//        //lần cuối nạp thì nạp bao nhiêu tiền?
//        fieldInputKinds.add(new FieldKind("created_date", "max"));
//        fieldDataKinds.add(new FieldKind("price", "sum"));
//        //lần nạp tối đa
//        fieldInputKinds.add(new FieldKind("null", "null"));
//        fieldDataKinds.add(new FieldKind("price", "max"));
//        //lần nạp tối thiểu
//        fieldInputKinds.add(new FieldKind("null", "null"));
//        fieldDataKinds.add(new FieldKind("price", "min"));
//        //đã xem bao nhiêu quảng cáo
//        fieldInputKinds.add(new FieldKind("null", "null"));
//        fieldDataKinds.add(new FieldKind("game_id", "count"));
//        //quảng cáo đã xem cho từng nền tảng quảng cáo
//        fieldInputKinds.add(new FieldKind("ad_where", "group by"));
//        fieldDataKinds.add(new FieldKind("game_id", "count"));
//        //chơi đến level bao nhiêu
//        fieldInputKinds.add(new FieldKind("null", "null"));
//        fieldDataKinds.add(new FieldKind("level_level", "max"));
//        //tổng cộng đã fail bao nhiêu lần?
//        fieldInputKinds.add(new FieldKind("status", "fail"));
//        fieldDataKinds.add(new FieldKind("null", "count"));
//        //lần gần nhất đăng nhập là bao giờ
//        fieldInputKinds.add(new FieldKind("null", "null"));
//        fieldDataKinds.add(new FieldKind("created_date", "max"));
//        //lần gần nhất chơi trong bao lâu
//        fieldInputKinds.add(new FieldKind("created_date", "max"));
//        fieldDataKinds.add(new FieldKind("session_time", "sum"));
//        //tổng thời gian chơi
//        fieldInputKinds.add(new FieldKind("null", "null"));
//        fieldDataKinds.add(new FieldKind("session_time", "sum"));
//        fieldTasks.add(new FieldTask("api_inapp_log_raw_data",fieldInputKinds.get(0),fieldDataKinds.get(0),"count_charge"));
//        fieldTasks.add(new FieldTask("api_inapp_log_raw_data",fieldInputKinds.get(1),fieldDataKinds.get(1),"total_charge"));
//        fieldTasks.add(new FieldTask("api_inapp_log_raw_data",fieldInputKinds.get(2),fieldDataKinds.get(2),"last_time_charge"));
//        fieldTasks.add(new FieldTask("api_inapp_log_raw_data",fieldInputKinds.get(3),fieldDataKinds.get(3),"last_money_charge"));
//        fieldTasks.add(new FieldTask("api_inapp_log_raw_data",fieldInputKinds.get(4),fieldDataKinds.get(4),"max_charge"));
//        fieldTasks.add(new FieldTask("api_inapp_log_raw_data",fieldInputKinds.get(5),fieldDataKinds.get(5),"min_charge"));
//        fieldTasks.add(new FieldTask("api_ads_log_raw_data",fieldInputKinds.get(6),fieldDataKinds.get(6),"count_ads_view"));
//        fieldTasks.add(new FieldTask("api_ads_log_raw_data",fieldInputKinds.get(7),fieldDataKinds.get(7),"count_ads_view_for"));
//        fieldTasks.add(new FieldTask("api_level_log_raw_data",fieldInputKinds.get(8),fieldDataKinds.get(8),"max_level"));
//        fieldTasks.add(new FieldTask("api_level_log_raw_data",fieldInputKinds.get(9),fieldDataKinds.get(9),"count_fail"));
//        fieldTasks.add(new FieldTask("api_retention_raw_data",fieldInputKinds.get(10),fieldDataKinds.get(10),"last_time_play"));
//        fieldTasks.add(new FieldTask("api_session_raw_data",fieldInputKinds.get(11),fieldDataKinds.get(11),"last_total_play"));
//        fieldTasks.add(new FieldTask("api_session_raw_data",fieldInputKinds.get(12),fieldDataKinds.get(12),"total_play"));
//        Task task=new Task(fieldTasks,"account_id");
//        preRun();
//        preTask(task);
//
//    }
}
