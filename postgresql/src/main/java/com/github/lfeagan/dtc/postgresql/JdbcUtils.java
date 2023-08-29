package com.github.lfeagan.dtc.postgresql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class JdbcUtils {

    public static void closeWithoutException(Statement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (Exception e) {
                // do nothing
            }
        }
    }


    public static void closeWithoutException(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                // do nothing
            }
        }
    }


    public static void closeWithoutException(ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (Exception e) {
                // do nothing
            }
        }
    }
}
