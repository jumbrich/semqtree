/*
 * Project: SmurfPDMS
 * Class: NetworkConfigException
 * File: NetworkConfigException
 * Date: 31. M�rz 2005; 10:00:18
 * Author: Andreas Job
 */

package de.ilmenau.datasum.exception;

/**
 * Exception which is thrown on errors while searching for a network configuration.
 */
@SuppressWarnings("serial")
public class QTreeException extends Exception
{private static final long serialVersionUID = 1L;
	/**
     *  Constructs an Exception with no specified detail message.
     */
    public QTreeException()
    {
            super();
    }

    /**
     *  Constructs an Exception with the specified detail message.
     *
     *@param  message the detail message.
     */
    public QTreeException(String message)
    {
            super(message);
    }
    
    /**
     *    Constructs a new exception with the specified detail message and cause.
     *
     *@param  cause the cause (which is saved for later retrieval by the 
     *         Throwable.getCause() method).
     *         (A null value is permitted, and indicates that the 
     *         cause is nonexistent or unknown.)
     */
    public QTreeException(Throwable cause)
    {
            super( cause );
    }

    /**
     *    Constructs a new exception with the specified detail message and cause.
     *
     *@param  message message the detail message
     *@param  cause the cause (which is saved for later retrieval by the 
     *         Throwable.getCause() method).
     *         (A null value is permitted, and indicates that the 
     *         cause is nonexistent or unknown.)
     */
    public QTreeException(String message,Throwable cause)
    {
            super(message, cause );
    }
}
