package com.example.datamart.data.task;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.*;

/**
 * @author QuangNN
 */
@Data
@AllArgsConstructor
public class Task {
    private String schema;
    private String mainField;//acount_id
    private List<TaskType1>taskType1s;
    private List<TaskType2>taskType2s;
    private List<TaskType3>taskType3s;
    private List<TaskType4>taskType4s;
}
