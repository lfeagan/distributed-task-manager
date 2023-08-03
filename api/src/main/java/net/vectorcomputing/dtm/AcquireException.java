package net.vectorcomputing.dtm;

public class AcquireException extends TaskManagerException {

    public AcquireException() {
        super();
    }

    public AcquireException(Task[] relatedTasks, TaskQuery relatedTaskQuery) {
        super(relatedTasks, relatedTaskQuery);
    }

    public AcquireException(String message) {
        super(message);
    }

    public AcquireException(String message, Throwable cause) {
        super(message, cause);
    }

    public AcquireException(Throwable cause) {
        super(cause);
    }

    public AcquireException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
