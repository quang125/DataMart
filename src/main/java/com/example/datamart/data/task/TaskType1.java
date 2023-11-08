package com.example.datamart.data.task;

import lombok.AllArgsConstructor;
import lombok.Data;
import java.util.*;
/**
 * @author QuangNN
 */
@Data
@AllArgsConstructor
public class TaskType1 {
    private String table;
    private List<String> groupFiles;
    private List<String> dataFields;
}
