package com.github.lfeagan.dtc;

public class TaskManagerException extends Exception {

    private final Task[] relatedTasks;
    private final TaskQuery relatedTaskQuery ;

    public Task[] getRelatedTasks() {
        return this.relatedTasks;
    }

    public TaskQuery getRelatedTaskQuery() {
        return this.relatedTaskQuery;
    }

    public TaskManagerException() {
        this.relatedTasks = null;
        this.relatedTaskQuery = null;
    }

    public TaskManagerException(Task[] relatedTasks, TaskQuery relatedTaskQuery) {
        this.relatedTasks = relatedTasks;
        this.relatedTaskQuery = relatedTaskQuery;
    }

    public TaskManagerException(String message) {
        super(message);
        this.relatedTasks = null;
        this.relatedTaskQuery = null;
    }

    public TaskManagerException(String message, Throwable cause) {
        super(message, cause);
        this.relatedTasks = null;
        this.relatedTaskQuery = null;
    }

    public TaskManagerException(Throwable cause) {
        super(cause);
        this.relatedTasks = null;
        this.relatedTaskQuery = null;
    }

    public TaskManagerException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
        this.relatedTasks = null;
        this.relatedTaskQuery = null;
    }
}
