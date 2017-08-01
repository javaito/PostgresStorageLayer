package org.hcjf.layers.storage.postgres.actions;

import org.hcjf.layers.storage.StorageAccessException;
import org.hcjf.layers.storage.actions.ResultSet;
import org.hcjf.layers.storage.actions.Update;
import org.hcjf.layers.storage.postgres.PostgresStorageSession;
import org.hcjf.layers.storage.postgres.properties.PostgresProperties;
import org.hcjf.log.Log;
import org.hcjf.properties.SystemProperties;
import org.hcjf.utils.Strings;

import java.sql.PreparedStatement;
import java.util.*;

/**
 * @author Javier Quiroga.
 */
public class PostgresUpdate extends Update<PostgresStorageSession> {

    private static final String UPDATE_STATEMENT = "UPDATE %s SET %s WHERE %s";

    public PostgresUpdate(PostgresStorageSession session) {
        super(session);
    }

    /**
     * Builds and executes the update sentence. Two parts are needed: the 'set' part and the 'where' part.
     * The 'set' part is build from specified values set.
     * The 'where' part is build from specified query.
     * @param params Query parameters
     * @param <R> Expected result set.
     * @return null
     * @throws StorageAccessException StorageAccessException
     */
    @Override
    public <R extends ResultSet> R execute(Object... params) throws StorageAccessException {
        try {
            R resultSet;
            String resourceName = getResourceName();
            if(getQuery() == null) {
                throw new StorageAccessException("Update query conditions not found");
            }
            if(getResourceName() == null) {
                resourceName = getQuery().getResourceName();
            }

            //List of values for all updates
            List<Object> baseValues = new ArrayList<>();
            //Creates the assignations body of the update operation.
            Strings.Builder setBuilder = new Strings.Builder();
            for(String fieldName : getValues().keySet()) {
                setBuilder.append(fieldName).append(Strings.ASSIGNATION).append(Strings.WHITE_SPACE);
                setBuilder.append(SystemProperties.get(SystemProperties.Query.ReservedWord.REPLACEABLE_VALUE), Strings.ARGUMENT_SEPARATOR, Strings.WHITE_SPACE);
                baseValues.add(getValues().get(fieldName).getValue());
            }

            //Creates the conditions body of the update operation.
            StringBuilder whereBuilder = new StringBuilder();
            if(getQuery().getEvaluators().size() == 0) {
                throw new StorageAccessException("Update query conditions not found");
            }
            whereBuilder = getSession().processEvaluators(whereBuilder, getQuery());

            //Creates statement string
            String statement = String.format(UPDATE_STATEMENT, resourceName, setBuilder.toString(), whereBuilder.toString());


            PreparedStatement preparedStatement = getSession().getConnection().prepareStatement(statement);
            int index = 1;
            for(Object baseValue : baseValues) {
                if (baseValue instanceof Date) {
                    preparedStatement.setTimestamp(index++, new java.sql.Timestamp(((Date) baseValue).getTime()));
                } else if (Collection.class.isAssignableFrom(baseValue.getClass())) {
                    for (Object object : ((Collection) baseValue)) {
                        preparedStatement.setObject(index++, object);
                    }
                } else {
                    preparedStatement.setObject(index++, baseValue);
                }
            }
            preparedStatement = getSession().setValues(preparedStatement, getQuery(), index, params);
            Log.d(SystemProperties.get(PostgresProperties.POSTGRES_EXECUTE_STATEMENT_LOG_TAG), preparedStatement.toString());
            preparedStatement.executeUpdate();
            return null;
        } catch (Exception ex) {
            getSession().onError(ex);
            throw new StorageAccessException(ex);
        }
    }
}
