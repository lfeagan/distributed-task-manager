package com.github.lfeagan.dtc;

public enum TaskStatus {
    AVAILABLE, // The value assigned when a task is created, or it has failed.
    ACQUIRED, // The value assigned when a task has been selected and is running.
    COMPLETE, // The value assigned when a task has finished without error and should not be processed again.
    SKIP; // A manually assigned value indicated that this task should not be processed

        /* Task State Transition Diagram
       CREATED --> RUNNING --> (COMPLETED|FAILED)
                     /\                |
                     |                 |
                     \-----------------/

       SKIP
     */
}
