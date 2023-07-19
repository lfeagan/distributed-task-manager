package net.vectorcomputing.dtm;

public enum TaskStatus {
    CREATED, // The value assigned when a task is created.
    RUNNING, // The value assigned when a task has been selected and is running.
    COMPLETED, // The value assigned when a task has finished without error and should not be processed again.
    FAILED, // The value assigned when a task has finished with error(s) and is a candidate for running again.
    SKIP; // A manually assigned value indicated that this task should not be processed

        /* Task State Transition Diagram
       CREATED --> RUNNING --> (COMPLETED|FAILED)
                     /\                |
                     |                 |
                     \-----------------/

       SKIP
     */
}
