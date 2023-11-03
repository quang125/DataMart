package com.example.datamart.data.task;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * @author QuangNN
 */
@Data
@AllArgsConstructor
public class TaskType4 {
    private String table;
    private List<String> dataFields;
    private String inputField;
    private String type;
}
