package com.example.datamart.service.monthly_task;

import com.example.datamart.data.TableInfo;
import com.example.datamart.repository.MongoRepository;
import com.example.datamart.service.RedshiftService;
import com.example.datamart.service.TaskService;
import com.example.datamart.utils.QueryGenerateUtil;
import jakarta.annotation.PostConstruct;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.checkerframework.common.value.qual.EnsuresMinLenIf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

/**
 * @author QuangNN
 */
@Component
public class NewBuyer implements InitializingBean {
    private static final RedshiftService redshiftDC2Service = new RedshiftService("jdbc:redshift://new-dwh-cluster.cbyg0igfhhw3.us-east-1.redshift.amazonaws.com:5439/dwh_games", "quangnn", "Yvx83kfRmHt42b6kqgM5gzjG6");
    private static final Map<String, List<String>> tableList = new HashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(TaskService.class);
    static Map<String, List<String>> res = new HashMap<>();
    static int fieldNumber;
    static int index = 0;
    List<String> fieldNames = new ArrayList<>(Arrays.asList("month","new_buyer","package_to_date","money_to_date","buyer_still_play_1","buyer_remove",
            "buyer_only_buy_in_month","package","money","buyer_still_play_2"));
    private static final String mainField="account_id";
    private static final String mainTable="fact_inapp";
    Map<String,List<String>>accountByMonth=new HashMap<>();
    List<String>distinctMonth;
    List<String> resourceApi=new ArrayList<>();
    @PostConstruct
    public void preRun() throws SQLException {
        List<TableInfo> tableInfoList = redshiftDC2Service.executeSelectAllTable("select * from svv_tables");
        for (TableInfo tableInfo : tableInfoList) {
            if (tableInfo.getSchemaName().startsWith("dwh_")) {
                if (!tableInfo.getSchemaName().equals("dwh_falcon_2")) continue;
                if (tableInfo.getTableName().startsWith("api_")) {
                    List<String> tables = tableList.getOrDefault(tableInfo.getSchemaName(), new ArrayList<>());
                    tables.add(tableInfo.getTableName());
                    tableList.put(tableInfo.getSchemaName(), tables);
                }
                if(tableInfo.getTableName().startsWith("api_resource")) resourceApi.add(tableInfo.getTableName());
            }
        }
        distinctMonth=redshiftDC2Service.executeSelectDistinct("select distinct left(created_day,7) from "+"dwh_falcon_2."+mainTable);
        distinctMonth.sort(String::compareTo);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Set<String>difMonth=new HashSet<>(distinctMonth);
        for(String month:distinctMonth){
            difMonth.remove(month);
            List<List<String>>list1=redshiftDC2Service.executeSelect(QueryGenerateUtil.queryMonthType1("dwh_falcon_2","fact_inapp",
                    Arrays.asList("account_id as a1", "count(*) as a2","sum(price_usd) as a3"),new ArrayList<>(difMonth)),Arrays.asList("b1","b2","b3"));
            List<List<String>>list2=redshiftDC2Service.executeSelect(QueryGenerateUtil.queryMonthType2("dwh_falcon_2","fact_inapp",month,
            Arrays.asList("account_id as a1", "count(*) as a2","sum(price_usd) as a3"),new ArrayList<>(difMonth)),Arrays.asList("b1","b2","b3"));
            List<List<String>>list3=redshiftDC2Service.executeSelect(QueryGenerateUtil.queryMonthType3("dwh_falcon_2","fact_inapp","account_id",
                    new ArrayList<>(difMonth)),Arrays.asList("count"));
            List<List<String>>list4=redshiftDC2Service.executeSelect(QueryGenerateUtil.queryMonthType4("dwh_falcon_2","fact_inapp",month,"account_id",
                    new ArrayList<>(difMonth)),Arrays.asList("count"));
            difMonth.add(month);
            List<String>l=new ArrayList<>();
            l.addAll(list1.get(0));
            l.addAll(list3.get(0));
            l.add(String.valueOf(Long.parseLong(list1.get(0).get(0))-Long.parseLong(list3.get(0).get(0))));
            l.addAll(list2.get(0));
            l.addAll(list4.get(0));
            res.put(month,l);
        }
        writeCsv();
    }
    public void writeCsv() {
        try (CSVPrinter csvPrinter = new CSVPrinter(new FileWriter("C://csv//month/test1.csv"), CSVFormat.DEFAULT)) {
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
