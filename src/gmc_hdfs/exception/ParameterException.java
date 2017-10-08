/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package gmc_hdfs.exception;


/**
 * This exception is to report bad or invalid parameters given during constructor.
 * 
 * @author Gokul Poduval
 * @author Chen-Khong Tham, National University of Singapore
 * @since CloudSim Toolkit 1.0
 */
public class ParameterException extends Exception {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 1L;

	/** The message. */
	private final String message;

	/**
	 * Constructs a new exception with <tt>null</tt> as its detail message.
	 * 
	 * @pre $none
	 * @post $none
	 */
	public ParameterException() {
		super();
		message = null;
	}

	/**
	 * Creates a new ParameterException object.
	 * 
	 * @param message an error message
	 * @pre $none
	 * @post $none
	 */
	public ParameterException(String message) {
		super();
		this.message = message;
	}

	/**
	 * Returns an error message of this object.
	 * 
	 * @return an error message
	 * @pre $none
	 * @post $none
	 */
	@Override
	public String toString() {
		return message;
	}

}

