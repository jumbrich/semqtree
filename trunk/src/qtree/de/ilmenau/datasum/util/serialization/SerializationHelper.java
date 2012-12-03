/*
 *
 */
package de.ilmenau.datasum.util.serialization;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * This class contains various helper method for serialization.
 * 
 * @author Christian Lemke
 * @version $Id$
 */
public final class SerializationHelper {
	private static final long serialVersionUID = 1L;
	/**
	 * 
	 * method that returns the serialized size of the given serializable object.
	 * 
	 * @param object the serializable object
	 * @return the size of the serialized object
	 * @throws IOException if serialization failed
	 */
	public static int getSerializedSizeOfObject(Serializable object) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(object);
		oos.close();
		return baos.toByteArray().length;
	}
}
