package com.example.datamart.data.task;

import lombok.AllArgsConstructor;
import lombok.Data;
import java.util.*;
/**
 * @author QuangNN
 */
@Data
@AllArgsConstructor
public class TaskType2 {
    private String table;
    private List<String> groupFields;
    private List<String> inputFields;
    private List<String> inputFieldValue;
    private List<String>datFields;
}
