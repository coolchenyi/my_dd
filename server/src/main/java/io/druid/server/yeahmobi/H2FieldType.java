package io.druid.server.yeahmobi;

/**
 * 
 * @author oscar.gao
 *
 */

/**
 * 
 * the h2 database table field types.
 * only use the following three types.
 */
public enum H2FieldType {

	/**
	 * Mapped to java.lang.String.
	 */
	VARCHAR, 
	
	/**
	 * Mapped to java.math.BigDecimal.
	 */
	DECIMAL,
	
	/**
	 * Mapped to java.lang.Long.
	 */
	BIGINT
}
