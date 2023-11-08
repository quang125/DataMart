package com.example.datamart.data.task;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * @author QuangNN
 */
@Data
@AllArgsConstructor
public class TaskType3 {
    private String table;
    private List<String> groupFields;
    private List<String> dataFields;
    private List<String> types;
    private List<String> dataTypes;
}
