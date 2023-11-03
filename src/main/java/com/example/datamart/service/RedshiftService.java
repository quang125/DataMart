package com.example.datamart.service;


import com.example.datamart.data.TableInfo;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.*;
import java.util.*;

/**
 * A class used to interact with redshift (could be used for other data sources too, but it hasn't been tested yet).
 *
 * @author LeeHuyyHoangg
 */
@SuppressWarnings("unused")
public class RedshiftService {
    private final HikariDataSource hikariDataSource;

    /**
     * The constructor create an instance, with fixed max connection size of 15 and max idle time of 0.3 seconds.
     *
     * @param redshiftUrl The url of the connection.
     * @param userName    The username used to log in to the db.
     * @param passWord    The password used to log in to the db.
     */
    public RedshiftService(String redshiftUrl, String userName, String passWord) {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName("com.amazon.redshift.jdbc42.Driver");

        config.setJdbcUrl(redshiftUrl);
        config.setUsername(userName);
        config.setPassword(passWord);

        config.setMaximumPoolSize(15);
        config.setAutoCommit(true);
        config.setIdleTimeout(30000);

        hikariDataSource = new HikariDataSource(config);
    }

    /**
     * Setting parameter of unknown type to a preparedStatement.
     *
     * @param statement The preparedStatement.
     * @param value     The parameter.
     * @param atIndex   The index of the variable symbol '?' corresponding with the parameter.
     * @throws SQLException An exception that provides information on a database access error or other errors.
     */
    private static void setParameterForStatement(PreparedStatement statement, Object value, int atIndex) throws SQLException {
        if (value == null) {
            statement.setString(atIndex, null);
        } else if (value instanceof Integer) {
            statement.setInt(atIndex, (int) value);
        } else if (value instanceof Long) {
            statement.setLong(atIndex, (long) value);
        } else if (value instanceof Float) {
            statement.setFloat(atIndex, (float) value);
        } else if (value instanceof Double) {
            statement.setDouble(atIndex, (double) value);
        } else if (value instanceof String) {
            statement.setString(atIndex, ((String) value).toLowerCase());
        } else {
            statement.setString(atIndex, value.toString().toLowerCase());
        }
    }

    /**
     * Check if the table exist in the db by testing querying "select  from {the table}" -> if no exception is thrown then the table exist.
     * This function does not guarantee the correctness, since the sqlException con be various.
     *
     * @param tableName Name of the table checking.
     * @return if the table exist or not.
     */
    public boolean tableExists(String tableName) {
        try (Connection connection = hikariDataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(String.format("select * from %s", tableName))) {

            statement.executeQuery();
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    /**
     * Execute only update commands in sql (not select), the sqlCommand and objects must be in the right order.
     *
     * @param sqlCommand The command with symbol '?' stand for variable.
     * @param objects    The variables, must not be null, if no variable required then just pass new Object[0].
     * @throws SQLException An exception that provides information on a database access error or other errors.
     */
    public void executeUpdate(String sqlCommand, Object[] objects) throws SQLException {
        try (Connection connection = hikariDataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sqlCommand)) {
            for (int i = 0; i < objects.length; i++) {
                setParameterForStatement(statement, objects[i], i + 1);
            }

            statement.executeUpdate();
        }
    }

    /**
     * Execute only update command in sql (not select), no variable allowed.
     *
     * @param sqlCommand The command with no variable.
     * @throws SQLException An exception that provides information on a database access error or other errors.
     */
    public void executeUpdate(String sqlCommand) throws SQLException {
        try (Connection connection = hikariDataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sqlCommand)) {
            statement.executeUpdate();
        }
    }

    /**
     * Execute an update command in sql (not select) multiple times, by replacing only the variable each time.
     * It can be considered to execute multiple time the function {@link #executeUpdate(String, Object[])} with same sqlCommand and different variables each time,
     * but faster and more resource saving since it create only one connection and one statement.
     *
     * @param sqlCommand  The command with symbol '?' stand for variable.
     * @param objectsList List of the variable array, must not be null, must not contain null value, each array is corresponding to the variable putted in the command.
     * @throws SQLException An exception that provides information on a database access error or other errors.
     */
    public void executeMultipleUpdate(String sqlCommand, List<Object[]> objectsList) throws SQLException {
        try (Connection connection = hikariDataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sqlCommand)) {
            for (Object[] objects : objectsList) {
                for (int i = 0; i < objects.length; i++) {
                    setParameterForStatement(statement, objects[i], i + 1);
                }
                statement.executeUpdate();
            }
        }
    }

    public String executeCountSelect(String sqlCommand) throws SQLException {
        try (Connection connection = hikariDataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sqlCommand)) {

            ResultSet resultSet = statement.executeQuery();

            while (resultSet.next()) {
                return resultSet.getString(1);
            }
        }
        return null;
    }
    public List<List<String>> executeSelect(String sqlCommand, List<String>fields){
        List<List<String>>results=new ArrayList<>();
        try (Connection connection = hikariDataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sqlCommand)) {

            ResultSet resultSet = statement.executeQuery();

            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (resultSet.next()) {
                List<String>result=new ArrayList<>();
                for (String field:fields) {
                    String value = resultSet.getString(field);
                    if(value!=null) result.add(value);
                    else result.add("0");
                }
                results.add(result);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return results;
    }

    public void close() {
        hikariDataSource.close();
    }

    public List<TableInfo> executeSelectAllTable(String sqlCommand) throws SQLException {
        List<TableInfo>tableInfos=new ArrayList<>();
        try (Connection connection = hikariDataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sqlCommand)) {

            ResultSet resultSet = statement.executeQuery();

            while (resultSet.next()) {
                tableInfos.add(new TableInfo(resultSet.getString("table_name"),resultSet.getString("table_schema")));
            }
        }
        return tableInfos;
    }
    public List<String> executeSelectDistinct(String sqlCommand){
        Set<String> s=new HashSet<>();
        try (Connection connection = hikariDataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sqlCommand)) {

            ResultSet resultSet = statement.executeQuery();

            while (resultSet.next()) {
                s.add(resultSet.getString(1));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return new ArrayList<>(s);
    }
}
