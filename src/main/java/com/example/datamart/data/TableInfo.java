package com.example.datamart.data;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author QuangNN
 */
@AllArgsConstructor
@Data
public class TableInfo {
    private String tableName;
    private String schemaName;
}
