/*
 *
 */
package de.ilmenau.datasum.util.bigmath;

/**
 * This class represents an exception that is thrown if the internal size of two {@link BigUInt}s
 * differ. That is because of the speed optimization.
 * 
 * @author Christian Lemke
 * @version $Id$
 */
@SuppressWarnings("serial")
public final class DifferentByteSizeException extends Exception {

	// no special constructors needed

}