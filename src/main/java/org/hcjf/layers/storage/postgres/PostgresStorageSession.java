package org.hcjf.layers.storage.postgres;

import org.hcjf.errors.Errors;
import org.hcjf.layers.query.*;
import org.hcjf.layers.storage.StorageAccessException;
import org.hcjf.layers.storage.StorageSession;
import org.hcjf.layers.storage.actions.CollectionResultSet;
import org.hcjf.layers.storage.actions.MapResultSet;
import org.hcjf.layers.storage.actions.Select;
import org.hcjf.layers.storage.actions.Update;
import org.hcjf.layers.storage.postgres.actions.PostgresSelect;
import org.hcjf.layers.storage.postgres.actions.PostgresUpdate;
import org.hcjf.layers.storage.postgres.errors.PostgressErrors;
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
                        Errors.getMessage(PostgressErrors.ROLLBACK_OPERATION), throwable);
            } else {
                try {
                    getConnection().commit();
                } catch (Exception ex){}
            }
            getConnection().close();
        } catch (SQLException ex) {
            Log.w(SystemProperties.get(PostgresProperties.POSTGRES_STORAGE_LAYER_LOG_TAG),
                    Errors.getMessage(PostgressErrors.UNABLE_TO_CLOSE_CONNECTION), ex);
            throw new IOException(Errors.getMessage(PostgressErrors.UNABLE_TO_CLOSE_CONNECTION), ex);
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
    public <R extends org.hcjf.layers.storage.actions.ResultSet> R createResultSet(Query query, java.sql.ResultSet sqlResultSet, Class resultType)
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
                        mapResult.put(query.getResourceName() + Strings.CLASS_SEPARATOR + ((Query.QueryField)queryField).getFieldName(),
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
                for (int columnNumber = 1; columnNumber <= resultSetMetaData.getColumnCount(); columnNumber++) {
                    try {
                        Query.QueryComponent queryField = normalizeDataSourceToApplication(new Query.QueryField(
                                resultSetMetaData.getTableName(columnNumber) +
                                        Strings.CLASS_SEPARATOR + resultSetMetaData.getColumnLabel(columnNumber)));
                        if (queryField != null && setters.containsKey(((Query.QueryField) queryField).getFieldName())) {
                            setters.get(((Query.QueryField) queryField).getFieldName()).invoke(object, getValueFromColumn(sqlResultSet.getObject(columnNumber)));
                        }
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
     * Put into the query builder all the restrictions based on collection evaluator.
     * @param result Query builder.
     * @param collection Evaluator collection.
     * @return Query builder.
     */
    public StringBuilder processEvaluators(StringBuilder result, EvaluatorCollection collection) {
        String separatorValue = collection instanceof Or ?
                SystemProperties.get(SystemProperties.Query.ReservedWord.OR) :
                SystemProperties.get(SystemProperties.Query.ReservedWord.AND);
        boolean addSeparator = false;
        for(Evaluator evaluator : collection.getEvaluators()) {
            if(addSeparator) {
                result.append(Strings.WHITE_SPACE).append(separatorValue).append(Strings.WHITE_SPACE);
            }
            if(evaluator instanceof Or) {
                result.append(Strings.START_GROUP);
                processEvaluators(result, (Or)evaluator);
                result.append(Strings.END_GROUP);
            } else if(evaluator instanceof And) {
                result.append(Strings.START_GROUP);
                processEvaluators(result, (And)evaluator);
                result.append(Strings.END_GROUP);
            } else if(evaluator instanceof FieldEvaluator) {
                result.append(normalizeApplicationToDataSource(
                        ((FieldEvaluator)evaluator).getQueryParameter())).append(Strings.WHITE_SPACE);
                int size = 0;
                if(evaluator instanceof Distinct) {
                    if(((FieldEvaluator)evaluator).getRawValue() == null) {
                        result.append(SystemProperties.get(PostgresProperties.ReservedWord.IS_NOT_NULL_OPERATOR));
                        size = -1;
                    } else {
                        result.append(SystemProperties.get(SystemProperties.Query.ReservedWord.DISTINCT));
                    }
                } else if(evaluator instanceof Equals) {
                    if(((FieldEvaluator)evaluator).getRawValue() == null) {
                        result.append(SystemProperties.get(PostgresProperties.ReservedWord.IS_NULL_OPERATOR));
                        size = -1;
                    } else {
                        result.append(SystemProperties.get(SystemProperties.Query.ReservedWord.EQUALS));
                    }
                } else if(evaluator instanceof GreaterThanOrEqual) {
                    result.append(SystemProperties.get(SystemProperties.Query.ReservedWord.GREATER_THAN_OR_EQUALS));
                } else if(evaluator instanceof GreaterThan) {
                    result.append(SystemProperties.get(SystemProperties.Query.ReservedWord.GREATER_THAN));
                } else if(evaluator instanceof NotIn) {
                    result.append(SystemProperties.get(SystemProperties.Query.ReservedWord.NOT_IN));
                    if(((FieldEvaluator)evaluator).getRawValue() instanceof Collection) {
                        size = ((Collection) ((FieldEvaluator) evaluator).getRawValue()).size();
                    } else {
                        size = 1;
                    }
                } else if(evaluator instanceof In) {
                    result.append(SystemProperties.get(SystemProperties.Query.ReservedWord.IN));
                    if(((FieldEvaluator)evaluator).getRawValue() instanceof Collection) {
                        size = ((Collection) ((FieldEvaluator) evaluator).getRawValue()).size();
                    } else {
                        size = 1;
                    }
                } else if(evaluator instanceof Like) {
                    result.append(SystemProperties.get(PostgresProperties.ReservedWord.LIKE_OPERATOR));
                } else if(evaluator instanceof SmallerThanOrEqual) {
                    result.append(SystemProperties.get(SystemProperties.Query.ReservedWord.SMALLER_THAN_OR_EQUALS));
                } else if(evaluator instanceof SmallerThan) {
                    result.append(SystemProperties.get(SystemProperties.Query.ReservedWord.SMALLER_THAN));
                }
                if(size > 0) {
                    String argumentSeparatorValue = SystemProperties.get(SystemProperties.Query.ReservedWord.ARGUMENT_SEPARATOR);
                    String argumentSeparator = Strings.EMPTY_STRING;
                    result.append(Strings.WHITE_SPACE).append(Strings.START_GROUP);
                    for (int i = 0; i < size; i++) {
                        result.append(argumentSeparator).append(SystemProperties.get(SystemProperties.Query.ReservedWord.REPLACEABLE_VALUE));
                        argumentSeparator = argumentSeparatorValue;
                    }
                    result.append(Strings.END_GROUP).append(Strings.WHITE_SPACE);
                } else if(size == 0) {
                    result.append(Strings.WHITE_SPACE).append(SystemProperties.get(SystemProperties.Query.ReservedWord.REPLACEABLE_VALUE));
                } else {
                    result.append(Strings.WHITE_SPACE);
                }
            }
            addSeparator = true;
        }
        return result;
    }

    /**
     * Set the values for the prepared statement.
     * @param statement Prepared statement.
     * @param collection Evaluator collection.
     * @param index Starting index of the parameters.
     * @param params Execution parameters.
     * @return Prepared statement.
     */
    public PreparedStatement setValues(PreparedStatement statement, EvaluatorCollection collection, Integer index, Object... params) {
        Object value;
        for(Evaluator evaluator : collection.getEvaluators()) {
            if(evaluator instanceof Or) {
                statement = setValues(statement, (Or)evaluator, index, params);
            } else if(evaluator instanceof And) {
                statement = setValues(statement, (And)evaluator, index, params);
            } else if(evaluator instanceof FieldEvaluator) {
                try {
                    value = ((FieldEvaluator)evaluator).getValue(null,null, params);
                    if(value != null) {
                        if (value instanceof Date) {
                            statement.setTimestamp(index++, new java.sql.Timestamp(((Date) value).getTime()));
                        } else if (Collection.class.isAssignableFrom(value.getClass())) {
                            for (Object object : ((Collection) value)) {
                                statement.setObject(index++, object);
                            }
                        } else {
                            statement.setObject(index++, value);
                        }
                    }
                } catch (SQLException ex) {
                    throw new IllegalArgumentException(ex);
                }
            }
        }
        return statement;
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

    /**
     * Returns the update operation implementation.
     * @param query Query to filter the update.
     * @param values Values to be updated.
     * @return update operation.
     * @throws StorageAccessException
     */
    @Override
    public Update update(Query query, Map<String, Object> values) throws StorageAccessException {
        PostgresUpdate update = new PostgresUpdate(this);
        update.setQuery(query);
        for(String key : values.keySet()) {
            update.add(key, values.get(key));
        }
        return update;
    }
}
