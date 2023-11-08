package com.example.datamart.data.task;

import lombok.AllArgsConstructor;
import lombok.Data;
import java.util.*;
/**
 * @author QuangNN
 */
@AllArgsConstructor
@Data
public class TaskType5 {
    private String table;
    private List<String>groupFields;
    private List<String>dataFields;
}
