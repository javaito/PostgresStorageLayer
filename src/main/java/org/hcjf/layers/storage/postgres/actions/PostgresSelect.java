package org.hcjf.layers.storage.postgres.actions;

import org.hcjf.layers.query.*;
import org.hcjf.layers.storage.StorageAccessException;
import org.hcjf.layers.storage.actions.ResultSet;
import org.hcjf.layers.storage.actions.Select;
import org.hcjf.layers.storage.postgres.PostgresStorageSession;
import org.hcjf.layers.storage.postgres.properties.PostgresProperties;
import org.hcjf.log.Log;
import org.hcjf.properties.SystemProperties;
import org.hcjf.utils.Strings;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLType;
import java.util.Collection;
import java.util.Date;

/**
 * Select implementation for postgres database.
 * @author javaito.
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
            Query.QueryComponent normalizedQueryField;
            if(!query.returnAll()) {
                for (Query.QueryReturnParameter queryField : query.getReturnParameters()) {
                    queryBuilder.append(argumentSeparator);
                    normalizedQueryField = getSession().normalizeApplicationToDataSource(queryField);
                    queryBuilder.append(normalizedQueryField);
                    if (normalizedQueryField instanceof Query.QueryReturnParameter &&
                            ((Query.QueryReturnParameter) normalizedQueryField).getAlias() != null &&
                            !((Query.QueryReturnParameter) normalizedQueryField).getAlias().isEmpty()) {
                        queryBuilder.append(Strings.WHITE_SPACE);
                        queryBuilder.append(SystemProperties.get(SystemProperties.Query.ReservedWord.AS));
                        queryBuilder.append(Strings.WHITE_SPACE);
                        queryBuilder.append(((Query.QueryReturnParameter) normalizedQueryField).getAlias());
                    }
                    queryBuilder.append(Strings.WHITE_SPACE);
                    argumentSeparator = argumentSeparatorValue;
                }
            } else {
                queryBuilder.append(SystemProperties.get(SystemProperties.Query.ReservedWord.RETURN_ALL));
                queryBuilder.append(Strings.WHITE_SPACE);
            }
            queryBuilder.append(SystemProperties.get(SystemProperties.Query.ReservedWord.FROM)).append(Strings.WHITE_SPACE);
            queryBuilder.append(getSession().normalizeApplicationToDataSource(query.getResource())).append(Strings.WHITE_SPACE);

            if (query.getJoins() != null && query.getJoins().size() > 0) {
                for (Join join : query.getJoins()) {
                    switch(join.getType()) {
                        case JOIN:
                            queryBuilder.append(SystemProperties.get(SystemProperties.Query.ReservedWord.JOIN));
                            break;
                        case LEFT:
                            queryBuilder.append(SystemProperties.get(SystemProperties.Query.ReservedWord.LEFT));
                            queryBuilder.append(Strings.WHITE_SPACE);
                            queryBuilder.append(SystemProperties.get(SystemProperties.Query.ReservedWord.JOIN));
                            break;
                        case RIGHT:
                            queryBuilder.append(SystemProperties.get(SystemProperties.Query.ReservedWord.RIGHT));
                            queryBuilder.append(Strings.WHITE_SPACE);
                            queryBuilder.append(SystemProperties.get(SystemProperties.Query.ReservedWord.JOIN));
                            break;
                        case INNER:
                            queryBuilder.append(SystemProperties.get(SystemProperties.Query.ReservedWord.INNER));
                            queryBuilder.append(Strings.WHITE_SPACE);
                            queryBuilder.append(SystemProperties.get(SystemProperties.Query.ReservedWord.JOIN));
                            break;
                    }
                    queryBuilder.append(Strings.WHITE_SPACE);
                    queryBuilder.append(join.getResourceName());
                    queryBuilder.append(Strings.WHITE_SPACE);
                    queryBuilder.append(SystemProperties.get(SystemProperties.Query.ReservedWord.ON));
                    queryBuilder.append(Strings.WHITE_SPACE);
                    queryBuilder.append(join.getLeftField().getCompleteFieldName());
                    queryBuilder.append(SystemProperties.get(SystemProperties.Query.ReservedWord.EQUALS));
                    queryBuilder.append(join.getRightField().getCompleteFieldName());
                    queryBuilder.append(Strings.WHITE_SPACE);
                }
            }

            if(query.getEvaluators().size() > 0) {
                queryBuilder.append(SystemProperties.get(SystemProperties.Query.ReservedWord.WHERE));
                queryBuilder.append(Strings.WHITE_SPACE);
                queryBuilder = getSession().processEvaluators(queryBuilder, query);
                queryBuilder.append(Strings.WHITE_SPACE);
            }

            if(query.getGroupParameters().size() > 0) {
                queryBuilder.append(SystemProperties.get(SystemProperties.Query.ReservedWord.GROUP_BY));
                argumentSeparator = Strings.EMPTY_STRING;
                for (Query.QueryReturnParameter groupParameter: query.getGroupParameters()) {
                    queryBuilder.append(argumentSeparator).append(Strings.WHITE_SPACE).append(getSession().normalizeApplicationToDataSource(groupParameter));
                    argumentSeparator = argumentSeparatorValue;
                }
                queryBuilder.append(Strings.WHITE_SPACE);
            }

            if(query.getOrderParameters().size() > 0) {
                queryBuilder.append(SystemProperties.get(SystemProperties.Query.ReservedWord.ORDER_BY));
                argumentSeparator = Strings.EMPTY_STRING;
                for (Query.QueryOrderParameter orderParameter: query.getOrderParameters()) {
                    queryBuilder.append(argumentSeparator).append(Strings.WHITE_SPACE).append(getSession().normalizeApplicationToDataSource(orderParameter));
                    if(orderParameter.isDesc()) {
                        queryBuilder.append(Strings.WHITE_SPACE).append(SystemProperties.get(SystemProperties.Query.ReservedWord.DESC));
                    }
                    argumentSeparator = argumentSeparatorValue + Strings.WHITE_SPACE;
                }
                queryBuilder.append(Strings.WHITE_SPACE);
            }

            if(query.getLimit() != null) {
                queryBuilder.append(SystemProperties.get(SystemProperties.Query.ReservedWord.LIMIT)).append(Strings.WHITE_SPACE).append(query.getLimit());
            }

            PreparedStatement preparedStatement = getSession().getConnection().prepareStatement(queryBuilder.toString());
            preparedStatement = getSession().setValues(preparedStatement, query, 1, params);
            Log.d(SystemProperties.get(PostgresProperties.POSTGRES_EXECUTE_STATEMENT_LOG_TAG), preparedStatement.toString());
            return getSession().createResultSet(getQuery(), preparedStatement.executeQuery(), getResultType());
        } catch (Exception ex) {
            getSession().onError(ex);
            throw new StorageAccessException(ex);
        }
    }


}
