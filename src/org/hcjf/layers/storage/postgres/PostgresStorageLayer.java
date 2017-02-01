package org.hcjf.layers.storage.postgres;

import org.hcjf.layers.storage.StorageLayer;
import org.hcjf.layers.storage.postgres.errors.Errors;
import org.hcjf.layers.storage.postgres.properties.PostgresProperties;
import org.hcjf.log.Log;
import org.hcjf.properties.SystemProperties;
import org.postgresql.ds.PGPoolingDataSource;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Base layer to create a pooling connection with a postgres data base engine.
 * @author Javier Quiroga.
 * @email javier.quiroga@sitrack.com
 */
public abstract class PostgresStorageLayer extends StorageLayer<PostgresStorageSession> {

    private PGPoolingDataSource source;

    public PostgresStorageLayer(String implName) {
        super(implName);
    }

    /**
     * The first time this method creates a postgres pooling data source, then
     * only return a session with a postgres connection.
     * @return Postgres storage session.
     */
    @Override
    public PostgresStorageSession begin() {
        synchronized (this) {
            if(source == null) {
                source = new PGPoolingDataSource();
                source.setDataSourceName(getDataSourceName());
                source.setServerName(getServerName());
                source.setDatabaseName(getDatabaseName());
                source.setUser(getUserName());
                source.setPassword(getPassword());
                source.setInitialConnections(getInitialConnections());
                source.setMaxConnections(getMaxConnections());
                source.setPortNumber(getPortNumber());

                try {
                    Connection connection = source.getConnection();
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        try {
            Connection connection = source.getConnection();
            connection.setAutoCommit(false);
            return new PostgresStorageSession(getImplName(), connection);
        } catch (SQLException ex) {
            Log.e(SystemProperties.get(PostgresProperties.POSTGRES_STORAGE_LAYER_LOG_TAG),
                    Errors.getError(Errors.UNABLE_TO_CREATE_CONNECTION), ex);
            throw new RuntimeException(Errors.getError(Errors.UNABLE_TO_CREATE_CONNECTION), ex);
        }
    }

    /**
     * Return a name for a data source.
     * @return Data source name.
     */
    protected abstract String getDataSourceName();

    /**
     * Return the host of the data base engine.
     * @return Data base engine host.
     */
    protected abstract String getServerName();

    /**
     * Return the data base name.
     * @return Data base name.
     */
    protected abstract String getDatabaseName();

    /**
     * Return the user name.
     * @return User name.
     */
    protected abstract String getUserName();

    /**
     * Return the password.
     * @return Password.
     */
    protected abstract String getPassword();

    /**
     * Return the initial connection size for the pool.
     * @return Initial connection size.
     */
    protected abstract Integer getInitialConnections();

    /**
     * Return the max connection size for the pool.
     * @return Max connection size.
     */
    protected abstract Integer getMaxConnections();

    /**
     * Return the port number of the server.
     * @return Port number.
     */
    protected abstract Integer getPortNumber();

}
