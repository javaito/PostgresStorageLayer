package org.hcjf.layers.storage.postgres.errors;

import org.hcjf.utils.Messages;

/**
 * @author Javier Quiroga.
 * @email javier.quiroga@sitrack.com
 */
public class Errors extends Messages {

    public static final String UNABLE_TO_CLOSE_CONNECTION = "postgres.errors.unable.to.close.connection";
    public static final String UNABLE_TO_CREATE_CONNECTION = "postgres.errors.unable.to.create.connection";
    public static final String ROLLBACK_OPERATION = "postgres.errors.rollback.operation";

    private static final Errors instance;

    static {
        instance = new Errors();
        instance.addInternalDefault(UNABLE_TO_CLOSE_CONNECTION, "Unable to close connection");
        instance.addInternalDefault(UNABLE_TO_CREATE_CONNECTION, "Unable to create connection");
        instance.addInternalDefault(ROLLBACK_OPERATION, "Rollback operation by session error");
    }

    public static String getError(String errorCode, Object... params) {
        return instance.getInternalMessage(errorCode, params);
    }
}
