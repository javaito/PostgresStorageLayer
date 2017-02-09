package org.hcjf.layers.storage.postgres;

import org.hcjf.layers.query.JoinableMap;
import org.hcjf.layers.query.Query;
import org.hcjf.layers.storage.StorageAccessException;
import org.hcjf.layers.storage.StorageSession;
import org.hcjf.layers.storage.actions.CollectionResultSet;
import org.hcjf.layers.storage.actions.MapResultSet;
import org.hcjf.layers.storage.actions.ResultSet;
import org.hcjf.layers.storage.actions.Select;
import org.hcjf.layers.storage.postgres.actions.PostgresSelect;
import org.hcjf.layers.storage.postgres.errors.Errors;
import org.hcjf.layers.storage.postgres.properties.PostgresProperties;
import org.hcjf.log.Log;
import org.hcjf.properties.SystemProperties;
import org.hcjf.utils.Introspection;
import org.hcjf.utils.Strings;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.Date;

/**
 * This class implements the postgres session.
 * @author Javier Quiroga.
 * @email javier.quiroga@sitrack.com
 */
public class PostgresStorageSession extends StorageSession {

    private final Connection connection;
    private Throwable throwable;

    public PostgresStorageSession(String implName, Connection connection) {
        super(implName);
        this.connection = connection;
    }

    /**
     * Return the instance of the pooled postgres connection asocciated to the session.
     * @return Polled postgres connection.
     */
    public final Connection getConnection() {
        return connection;
    }

    /**
     * This method is callas when occurs an error in some operation over the session.
     * @param throwable Throwable that represents the error.
     */
    public final void onError(Throwable throwable) {
        this.throwable = throwable;
    }

    /**
     * Close the postgres connection, when the connection is closed its is released to the
     * connections pool.
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        try {
            if(throwable != null) {
                try {
                    getConnection().rollback();
                } catch (Exception ex) {}
                Log.w(SystemProperties.get(PostgresProperties.POSTGRES_STORAGE_LAYER_LOG_TAG),
                        Errors.getError(Errors.ROLLBACK_OPERATION), throwable);
            } else {
                try {
                    getConnection().commit();
                } catch (Exception ex){}
            }
            getConnection().close();
        } catch (SQLException ex) {
            Log.w(SystemProperties.get(PostgresProperties.POSTGRES_STORAGE_LAYER_LOG_TAG),
                    Errors.getError(Errors.UNABLE_TO_CLOSE_CONNECTION), ex);
            throw new IOException(Errors.getError(Errors.UNABLE_TO_CLOSE_CONNECTION), ex);
        }
    }

    /**
     * Creates a hcjf result set from a postgres data base result set.
     * @param query Query instance that was evaluated for postgres engine.
     * @param sqlResultSet Postgres result set.
     * @param resultType Expected object to create hcjf result set.
     * @param <R> Expected kind of result set.
     * @return Result set instance.
     * @throws SQLException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public <R extends ResultSet> R createResultSet(Query query, java.sql.ResultSet sqlResultSet, Class resultType)
            throws SQLException, IllegalAccessException, InstantiationException {
        ResultSetMetaData resultSetMetaData = sqlResultSet.getMetaData();
        R resultSet = null;
        Object columnValue;
        if(resultType == null) {
            List<Map<String, Object>> collectionResult = new ArrayList<>();
            while (sqlResultSet.next()) {
                JoinableMap mapResult = new JoinableMap(query.getResourceName());
                for (int columnNumber = 1; columnNumber <= resultSetMetaData.getColumnCount(); columnNumber++) {
                    Query.QueryComponent queryField = normalizeDataSourceToApplication(new Query.QueryField(
                            resultSetMetaData.getTableName(columnNumber) +
                                    Strings.CLASS_SEPARATOR + resultSetMetaData.getColumnLabel(columnNumber)));
                    if(queryField != null) {
                        mapResult.put(query.getResourceName() + Strings.CLASS_SEPARATOR + queryField.toString(),
                                getValueFromColumn(sqlResultSet.getObject(columnNumber)));
                    }
                }
                collectionResult.add(mapResult);
            }
            resultSet = (R) new MapResultSet(collectionResult);
        } else {
            Collection<Object> collectionResult = new ArrayList<>();
            Map<String, Introspection.Setter> setters = Introspection.getSetters(resultType);
            while (sqlResultSet.next()) {
                Object object = resultType.newInstance();
                for(String setterName : setters.keySet()) {
                    try {
                        int index = sqlResultSet.findColumn(
                                normalizeApplicationToDataSource(new Query.QueryField(
                                        setterName)).toString());
                        setters.get(setterName).invoke(object, getValueFromColumn(sqlResultSet.getObject(index)));
                    } catch (Exception ex){}
                }
                collectionResult.add(object);
            }
            resultSet = (R) new CollectionResultSet(collectionResult);
        }

        return resultSet;
    }

    /**
     * Mapping some kind of data type from data base to java types.
     * @param columnValue Value from data base column.
     * @return Java-friendly instance
     * @throws SQLException
     */
    protected Object getValueFromColumn(Object columnValue) throws SQLException {
        Object result = columnValue;
        if(columnValue != null) {
            if (columnValue instanceof Array) {
                result = Arrays.asList((Object[]) ((Array) columnValue).getArray());
            } else if (columnValue instanceof BigDecimal) {
                result = ((BigDecimal) columnValue).doubleValue();
            } else if (columnValue instanceof Timestamp) {
                result = new Date(((Timestamp) columnValue).getTime());
            }
        }
        return result;
    }

    /**
     * Return the select (postgres implementation) instance associated to the query parameter.
     * @param query Query parameter.
     * @return Select instance.
     * @throws StorageAccessException
     */
    @Override
    public Select select(Query query) throws StorageAccessException {
        Select select = new PostgresSelect(this);
        select.setQuery(query);
        return select;
    }

}
