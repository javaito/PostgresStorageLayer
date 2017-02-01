package org.hcjf.layers.storage.postgres;

import org.hcjf.layers.query.Query;
import org.hcjf.layers.storage.StorageAccessException;
import org.hcjf.layers.storage.StorageSession;
import org.hcjf.layers.storage.actions.Delete;
import org.hcjf.layers.storage.actions.Insert;
import org.hcjf.layers.storage.actions.Select;
import org.hcjf.layers.storage.actions.Update;
import org.hcjf.layers.storage.postgres.actions.PostgresSelect;
import org.hcjf.layers.storage.postgres.errors.Errors;
import org.hcjf.layers.storage.postgres.properties.PostgresProperties;
import org.hcjf.log.Log;
import org.hcjf.properties.SystemProperties;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

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
                getConnection().rollback();
                Log.w(SystemProperties.get(PostgresProperties.POSTGRES_STORAGE_LAYER_LOG_TAG),
                        Errors.getError(Errors.ROLLBACK_OPERATION), throwable);
            } else {
                getConnection().commit();
            }
            getConnection().close();
        } catch (SQLException ex) {
            Log.w(SystemProperties.get(PostgresProperties.POSTGRES_STORAGE_LAYER_LOG_TAG),
                    Errors.getError(Errors.UNABLE_TO_CLOSE_CONNECTION), ex);
            throw new IOException(Errors.getError(Errors.UNABLE_TO_CLOSE_CONNECTION), ex);
        }
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
