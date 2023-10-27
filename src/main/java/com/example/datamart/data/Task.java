package com.example.datamart.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import java.util.*;


/**
 * @author QuangNN
 */
@AllArgsConstructor
@Data
public class Task {
    private List<FieldTask> fieldTask;
    private String mainField;
}
