package org.hcjf.layers.storage.postgres.errors;

import org.hcjf.errors.Errors;
import org.hcjf.utils.Messages;

/**
 * @author Javier Quiroga.
 */
public class PostgressErrors {

    public static final String UNABLE_TO_CLOSE_CONNECTION = "postgres.errors.unable.to.close.connection";
    public static final String UNABLE_TO_CREATE_CONNECTION = "postgres.errors.unable.to.create.connection";
    public static final String ROLLBACK_OPERATION = "postgres.errors.rollback.operation";

    public static void main() {
        Errors.addDefault(UNABLE_TO_CLOSE_CONNECTION, "Unable to close connection");
        Errors.addDefault(UNABLE_TO_CLOSE_CONNECTION, "Unable to close connection");
        Errors.addDefault(UNABLE_TO_CREATE_CONNECTION, "Unable to create connection");
        Errors.addDefault(ROLLBACK_OPERATION, "Rollback operation by session error");
    }

}
