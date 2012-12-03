/*
 * 
 */
package de.ilmenau.datasum.exception;

/**
 * @author karn
 *
 * To change the template for this generated type comment go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
@SuppressWarnings("serial")
public class NotYetImplementedException extends RuntimeException {
	private static final long serialVersionUID = 1L;
	/**
     *  Constructs an Exception with no specified detail message.
     */
	public NotYetImplementedException() {
		super();
	}

	
	/**
     *  Constructs an Exception with the specified detail message.
     *
     *@param  message the detail message.
     */
	public NotYetImplementedException(String message) {
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
	public NotYetImplementedException(Throwable cause) {
		super(cause);
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
	public NotYetImplementedException(String message, Throwable cause) {
		super(message, cause);
	}

}
