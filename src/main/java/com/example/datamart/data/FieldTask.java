package com.example.datamart.data;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.*;


/**
 * @author QuangNN
 */
@Data
@AllArgsConstructor
public class FieldTask {
    private String table;
    private FieldKind inputField;
    private FieldKind dataField;
    private String fieldName;
}
