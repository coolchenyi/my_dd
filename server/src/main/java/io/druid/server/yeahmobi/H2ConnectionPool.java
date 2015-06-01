package io.druid.server.yeahmobi;

/**
 * Created by oscar.gao on 8/4/14.
 */

import java.util.HashMap;
import java.util.Map;

import org.h2.jdbcx.JdbcConnectionPool;

/**
 * the pool of JDBC connection
 *
 */
public class H2ConnectionPool {

	/**
	 * the key is host + port, for example "127.0.0.1:9092"
	 */
	static Map<String, JdbcConnectionPool> cpMap = new HashMap<>();

	/**
	 * 
	 * @param ipAndPort should like: "127.0.0.1:9092"
	 * @return
	 */
	public static synchronized JdbcConnectionPool getPool(String ipAndPort) {
		if (null == cpMap.get(ipAndPort)) {
			String url = "jdbc:h2:tcp://" + ipAndPort
					+ "/mem:landingpage;DB_CLOSE_DELAY=-1";
			JdbcConnectionPool cp = JdbcConnectionPool.create(url, "sa", "");
			cpMap.put(ipAndPort, cp);
			return cp;
		} else {
			return cpMap.get(ipAndPort);
		}
	}
}
