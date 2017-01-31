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
 * @author Javier Quiroga.
 * @email javier.quiroga@sitrack.com
 */
public abstract class PostgresStorageLayer extends StorageLayer<PostgresStorageSession> {

    private final PGPoolingDataSource source;

    public PostgresStorageLayer(String implName) {
        super(implName);

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

    @Override
    public PostgresStorageSession begin() {
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

    protected abstract String getDataSourceName();

    protected abstract String getServerName();

    protected abstract String getDatabaseName();

    protected abstract String getUserName();

    protected abstract String getPassword();

    protected abstract Integer getInitialConnections();

    protected abstract Integer getMaxConnections();

    protected abstract Integer getPortNumber();

}
