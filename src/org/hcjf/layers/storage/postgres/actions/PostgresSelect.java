package org.hcjf.layers.storage.postgres.actions;

import org.hcjf.layers.query.*;
import org.hcjf.layers.storage.StorageAccessException;
import org.hcjf.layers.storage.actions.CollectionResultSet;
import org.hcjf.layers.storage.actions.MapResultSet;
import org.hcjf.layers.storage.actions.ResultSet;
import org.hcjf.layers.storage.actions.Select;
import org.hcjf.layers.storage.postgres.PostgresStorageSession;
import org.hcjf.layers.storage.postgres.properties.PostgresProperties;
import org.hcjf.properties.SystemProperties;
import org.hcjf.utils.Introspection;
import org.hcjf.utils.Strings;

import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

/**
 * Select implementation for postgres database.
 * @author Javier Quiroga.
 * @email javier.quiroga@sitrack.com
 */
public class PostgresSelect extends Select<PostgresStorageSession> {

    public PostgresSelect(PostgresStorageSession session) {
        super(session);
    }

    /**
     * Creates a prepared statement from the internal query and execute this statement
     * into postgres engine and transform the postgres result set to a hcjf result set.
     * @param params Execution parameter.
     * @param <R> Expected result set.
     * @return Result set.
     * @throws StorageAccessException Throw this exception for any error executing the postgres select.
     */
    @Override
    public <R extends ResultSet> R execute(Object... params) throws StorageAccessException {
        try {
            Query query = getQuery();

            StringBuilder queryBuilder = new StringBuilder();
            queryBuilder.append(SystemProperties.get(SystemProperties.Query.ReservedWord.SELECT)).append(Strings.WHITE_SPACE);
            String argumentSeparatorValue = SystemProperties.get(SystemProperties.Query.ReservedWord.ARGUMENT_SEPARATOR);
            String argumentSeparator = Strings.EMPTY_STRING;
            for(Query.QueryField queryField : query.getReturnFields()) {
                queryBuilder.append(argumentSeparator);
                queryBuilder.append(normalizeApplicationToDataSource(queryField));
                queryBuilder.append(Strings.WHITE_SPACE);
                argumentSeparator = argumentSeparatorValue;
            }
            queryBuilder.append(SystemProperties.get(SystemProperties.Query.ReservedWord.FROM)).append(Strings.WHITE_SPACE);
            queryBuilder.append(normalizeApplicationToDataSource(query.getResource())).append(Strings.WHITE_SPACE);
            if(query.getEvaluators().size() > 0) {
                queryBuilder.append(SystemProperties.get(SystemProperties.Query.ReservedWord.WHERE));
                queryBuilder.append(Strings.WHITE_SPACE);
                queryBuilder = processEvaluators(queryBuilder, query);
                queryBuilder.append(Strings.WHITE_SPACE);
            }

            if(query.getOrderFields().size() > 0) {
                queryBuilder.append(SystemProperties.get(SystemProperties.Query.ReservedWord.ORDER_BY));
                queryBuilder.append(Strings.WHITE_SPACE);
                argumentSeparator = Strings.EMPTY_STRING;
                for (Query.OrderField orderField : query.getOrderFields()) {
                    queryBuilder.append(normalizeApplicationToDataSource(orderField.getQueryField())).append(argumentSeparator).append(Strings.WHITE_SPACE);
                    if(orderField.isDesc()) {
                        queryBuilder.append(SystemProperties.get(SystemProperties.Query.ReservedWord.DESC)).append(Strings.WHITE_SPACE);
                    }
                    argumentSeparator = argumentSeparatorValue;
                }
            }

            if(query.getLimit() != null) {
                queryBuilder.append(SystemProperties.get(SystemProperties.Query.ReservedWord.LIMIT)).append(Strings.WHITE_SPACE).append(query.getLimit());
            }

            PreparedStatement preparedStatement = getSession().getConnection().prepareStatement(queryBuilder.toString());
            preparedStatement = setValues(preparedStatement, query, 1, params);
            java.sql.ResultSet sqlResultSet = preparedStatement.executeQuery();
            ResultSetMetaData resultSetMetaData = sqlResultSet.getMetaData();

            R resultSet = null;
            if(getResultType() == null) {
                List<Map<String, Object>> collectionResult = new ArrayList<>();
                while (sqlResultSet.next()) {
                    Map<String, Object> mapResult = new HashMap<>();
                    for (int columnNumber = 1; columnNumber <= resultSetMetaData.getColumnCount(); columnNumber++) {
                        mapResult.put(normalizeDataSourceToApplication(new Query.QueryField(resultSetMetaData.getColumnLabel(columnNumber))).toString(),
                                sqlResultSet.getObject(columnNumber));
                    }
                    collectionResult.add(mapResult);
                }
                resultSet = (R) new MapResultSet(collectionResult);
            } else {
                Collection<Object> collectionResult = new ArrayList<>();
                Map<String, Introspection.Setter> setters = Introspection.getSetters(getResultType());
                while (sqlResultSet.next()) {
                    Object object = getResultType().newInstance();
                    for(String setterName : setters.keySet()) {
                        try {
                            int index = sqlResultSet.findColumn(
                                    normalizeApplicationToDataSource(new Query.QueryField(setterName)).toString());
                            setters.get(setterName).invoke(object, sqlResultSet.getObject(index));
                        } catch (Exception ex){}
                    }
                    collectionResult.add(object);
                }
                resultSet = (R) new CollectionResultSet(collectionResult);
            }

            return resultSet;
        } catch (Exception ex) {
            getSession().onError(ex);
            throw new StorageAccessException(ex);
        }
    }

    /**
     * Put into the query builder all the restrictions based on collection evaluator.
     * @param result Query builder.
     * @param collection Evaluator collection.
     * @return Query builder.
     */
    private StringBuilder processEvaluators(StringBuilder result, EvaluatorCollection collection) {
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
                        ((FieldEvaluator)evaluator).getQueryField())).append(Strings.WHITE_SPACE);
                if(evaluator instanceof Distinct) {
                    result.append(SystemProperties.get(SystemProperties.Query.ReservedWord.DISTINCT));
                } else if(evaluator instanceof Equals) {
                    result.append(SystemProperties.get(SystemProperties.Query.ReservedWord.EQUALS));
                } else if(evaluator instanceof GreaterThanOrEqual) {
                    result.append(SystemProperties.get(SystemProperties.Query.ReservedWord.GREATER_THAN_OR_EQUALS));
                } else if(evaluator instanceof GreaterThan) {
                    result.append(SystemProperties.get(SystemProperties.Query.ReservedWord.GREATER_THAN));
                } else if(evaluator instanceof NotIn) {
                    result.append(SystemProperties.get(SystemProperties.Query.ReservedWord.NOT_IN));
                } else if(evaluator instanceof In) {
                    result.append(SystemProperties.get(SystemProperties.Query.ReservedWord.IN));
                } else if(evaluator instanceof Like) {
                    result.append(SystemProperties.get(PostgresProperties.ReservedWord.LIKE_OPERATOR));
                } else if(evaluator instanceof SmallerThanOrEqual) {
                    result.append(SystemProperties.get(SystemProperties.Query.ReservedWord.SMALLER_THAN_OR_EQUALS));
                } else if(evaluator instanceof SmallerThan) {
                    result.append(SystemProperties.get(SystemProperties.Query.ReservedWord.SMALLER_THAN));
                }
                result.append(Strings.WHITE_SPACE).append(SystemProperties.get(SystemProperties.Query.ReservedWord.REPLACEABLE_VALUE));
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
    private PreparedStatement setValues(PreparedStatement statement, EvaluatorCollection collection, Integer index, Object... params) {
        for(Evaluator evaluator : collection.getEvaluators()) {
            if(evaluator instanceof Or) {
                statement = setValues(statement, (Or)evaluator, index, params);
            } else if(evaluator instanceof And) {
                statement = setValues(statement, (And)evaluator, index, params);
            } else if(evaluator instanceof FieldEvaluator) {
                try {
                    statement.setObject(index++, ((FieldEvaluator)evaluator).getValue(params));
                } catch (SQLException ex) {
                    throw new IllegalArgumentException(ex);
                }
            }
        }
        return statement;
    }
}
