package org.hcjf.layers.storage.postgres.properties;

import org.hcjf.properties.SystemProperties;

/**
 * @author Javier Quiroga.
 * @email javier.quiroga@sitrack.com
 */
public final class PostgresProperties {

    public static final String POSTGRES_STORAGE_LAYER_LOG_TAG = "postgres.storage.layer.log.tag";

    public static class ReservedWord {

        public static final String DISTINCT_OPERATOR = "postgres.storage.layer.distinct.operator";
        public static final String EQUALS_OPERATOR = "postgres.storage.layer.equals.operator";
        public static final String GRATER_THAN_OPERATOR = "postgres.storage.layer.greater.than.operator";
        public static final String SMALLER_THAN_OPERATOR = "postgres.storage.layer.smaller.than.operator";
        public static final String LIKE_OPERATOR = "postgres.storage.layer.like.operator";

    }

    public static void init() {
        SystemProperties.putDefaultValue(POSTGRES_STORAGE_LAYER_LOG_TAG, "Postgres");

        SystemProperties.putDefaultValue(ReservedWord.LIKE_OPERATOR, "LIKE");
    }

}
