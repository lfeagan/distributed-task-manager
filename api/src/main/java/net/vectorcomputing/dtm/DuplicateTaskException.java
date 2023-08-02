package net.vectorcomputing.dtm;

public class DuplicateTaskException extends TaskManagerException {
    public DuplicateTaskException() {
    }

    public DuplicateTaskException(String message) {
        super(message);
    }

    public DuplicateTaskException(String message, Throwable cause) {
        super(message, cause);
    }

    public DuplicateTaskException(Throwable cause) {
        super(cause);
    }

    public DuplicateTaskException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
