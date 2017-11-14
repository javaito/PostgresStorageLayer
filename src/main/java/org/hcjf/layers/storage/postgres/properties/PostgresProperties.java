package org.hcjf.layers.storage.postgres.properties;

import org.hcjf.properties.SystemProperties;

/**
 * @author Javier Quiroga.
 */
public final class PostgresProperties {

    public static final String POSTGRES_STORAGE_LAYER_LOG_TAG = "postgres.storage.layer.log.tag";
    public static final String POSTGRES_EXECUTE_STATEMENT_LOG_TAG = "postgres.execute.statement.log.tag";

    public static class ReservedWord {

        public static final String DISTINCT_OPERATOR = "postgres.storage.layer.distinct.operator";
        public static final String EQUALS_OPERATOR = "postgres.storage.layer.equals.operator";
        public static final String GRATER_THAN_OPERATOR = "postgres.storage.layer.greater.than.operator";
        public static final String SMALLER_THAN_OPERATOR = "postgres.storage.layer.smaller.than.operator";
        public static final String LIKE_OPERATOR = "postgres.storage.layer.like.operator";
        public static final String IS_NULL_OPERATOR = "postgres.storage.layer.is.null.operator";
        public static final String IS_NOT_NULL_OPERATOR = "postgres.storage.layer.is.not.null.operator";

    }

    public static class Pool {
        public static final String MAX_CONNECTIONS = "postgres.storage.layer.pool.max.connections";
        public static final String INIT_CONNECTIONS = "postgres.storage.layer.pool.init.connections";
        public static final String IDLE_TIMEOUT = "postgres.storage.layer.pool.idle.timeout";
        public static final String MAX_LIFE_TIME = "postgres.storage.layer.pool.max.life.time";
        public static final String SERVER_NAME_FIELD = "postgres.storage.layer.pool.server.name.field";
        public static final String DATABASE_NAME_FIELD = "postgres.storage.layer.pool.database.name.field";
        public static final String USER_FIELD = "postgres.storage.layer.pool.user.field";
        public static final String PASSWORD_FIELD = "postgres.storage.layer.pool.password.field";
        public static final String PORT_NUMBER_FIELD = "postgres.storage.layer.pool.port.number.field";
    }

    public static void init() {
        SystemProperties.putDefaultValue(POSTGRES_STORAGE_LAYER_LOG_TAG, "Postgres");
        SystemProperties.putDefaultValue(POSTGRES_EXECUTE_STATEMENT_LOG_TAG, "pgDB");

        SystemProperties.putDefaultValue(ReservedWord.LIKE_OPERATOR, "LIKE");
        SystemProperties.putDefaultValue(ReservedWord.IS_NULL_OPERATOR, "IS NULL");
        SystemProperties.putDefaultValue(ReservedWord.IS_NOT_NULL_OPERATOR, "IS NOT NULL");

        SystemProperties.putDefaultValue(Pool.MAX_CONNECTIONS, "5");
        SystemProperties.putDefaultValue(Pool.INIT_CONNECTIONS, "2");
        SystemProperties.putDefaultValue(Pool.IDLE_TIMEOUT, "30000");
        SystemProperties.putDefaultValue(Pool.MAX_LIFE_TIME, "120000");
        SystemProperties.putDefaultValue(Pool.SERVER_NAME_FIELD, "serverName");
        SystemProperties.putDefaultValue(Pool.DATABASE_NAME_FIELD, "databaseName");
        SystemProperties.putDefaultValue(Pool.USER_FIELD, "user");
        SystemProperties.putDefaultValue(Pool.PASSWORD_FIELD, "password");
        SystemProperties.putDefaultValue(Pool.PORT_NUMBER_FIELD, "portNumber");
    }

}
