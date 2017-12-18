package org.hcjf.layers.storage.postgres.actions;

import org.hcjf.layers.storage.StorageAccessException;
import org.hcjf.layers.storage.actions.Insert;
import org.hcjf.layers.storage.actions.ResultSet;
import org.hcjf.layers.storage.postgres.PostgresStorageSession;
import org.hcjf.layers.storage.postgres.properties.PostgresProperties;
import org.hcjf.layers.storage.values.StorageValue;
import org.hcjf.log.Log;
import org.hcjf.properties.SystemProperties;
import org.hcjf.utils.Strings;

import java.sql.PreparedStatement;
import java.util.*;

/**
 * @author Javier Quiroga.
 */
public class PostgresInsert extends Insert<PostgresStorageSession> {

    private static final String INSERT_STATEMENT = "INSERT INTO %s (%s) VALUES (%s);";

    public PostgresInsert(PostgresStorageSession session) {
        super(session);
    }

    @Override
    public <R extends ResultSet> R execute(Object... params) throws StorageAccessException {

        try {

            Strings.Builder valuesBuilder = new Strings.Builder();
            Strings.Builder valuePlacesBuilder = new Strings.Builder();
            List<Object> values = new ArrayList<>();

            Map<String, StorageValue> storageValues = getValues();
            for (String storageValueName : storageValues.keySet()) {
                valuesBuilder.append(storageValueName, Strings.ARGUMENT_SEPARATOR);
                valuePlacesBuilder.append(SystemProperties.get(SystemProperties.Query.ReservedWord.REPLACEABLE_VALUE), Strings.ARGUMENT_SEPARATOR);
                values.add(storageValues.get(storageValueName).getValue());
            }

            String statement = String.format(INSERT_STATEMENT, getResourceName(), valuesBuilder.toString(), valuePlacesBuilder.toString());
            PreparedStatement preparedStatement = getSession().getConnection().prepareStatement(statement);
            int index = 1;
            for (Object value : values) {
                if (value instanceof Date) {
                    preparedStatement.setTimestamp(index++, new java.sql.Timestamp(((Date) value).getTime()));
                } else if (Collection.class.isAssignableFrom(value.getClass())) {
                    for (Object object : ((Collection) value)) {
                        preparedStatement.setObject(index++, object);
                    }
                } else {
                    preparedStatement.setObject(index++, value);
                }
            }

            Log.d(SystemProperties.get(PostgresProperties.POSTGRES_EXECUTE_STATEMENT_LOG_TAG), preparedStatement.toString());
            preparedStatement.executeUpdate();
            return null;
        }
        catch (Exception ex) {
            getSession().onError(ex);
            throw new StorageAccessException(ex);
        }
    }
}