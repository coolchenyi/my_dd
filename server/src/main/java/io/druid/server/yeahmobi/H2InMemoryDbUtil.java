package io.druid.server.yeahmobi;

/**
 * Created by oscar.gao on 8/4/14.
 */

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

import com.metamx.emitter.EmittingLogger;

/**
 * H2 in-memory DB utility
 * 
 */
public class H2InMemoryDbUtil {

	/**
	 * first read the table definition from the meta table LP_META<br>
	 * then insert data into the landing page table according the table
	 * definition
	 * 
	 * @param dbAddress
	 * @param tableName
	 * @param rows
	 */
	public static void insertData(String dbAddress, String tableName,
			List<List<Object>> rows, LinkedHashMap<String, String> fieldTypeMap) {

		Connection conn = null;
		PreparedStatement pstmt = null;

		try {
			conn = H2ConnectionPool.getPool(dbAddress).getConnection();
			String sql = generateInsertSql(tableName, fieldTypeMap);
			pstmt = conn.prepareStatement(sql);
			for (List<Object> row : rows) {
				int i = 0;
				for (Entry<String, String> entry : fieldTypeMap.entrySet()) {

					Object column = row.get(i);
					String type = entry.getValue();
					if (type.equals("DECIMAL")) {
						pstmt.setBigDecimal(i + 1, (BigDecimal) column);
					} else if (type.equals("VARCHAR")) {
						pstmt.setString(i + 1, (String) column);
					} else if (type.equals("BIGINT")) {
						pstmt.setLong(i + 1, (Long) column);
					} else {
						String msg = String.format("error type %s", type);
						throw new RuntimeException(msg);
					}
					++i;
				}
				pstmt.addBatch();
			}

			pstmt.executeBatch();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			closeAutoCloseable(pstmt,
					"close database prepared statement failed");
			closeAutoCloseable(conn, "close database connection failed");
		}
	}

	/**
	 * get table field type map by query the LP_META table<Br>
	 * some field is BIGINT
	 * some field is VARCHAR 
	 * some field is DECIMAL
	 * 
	 * @param dbAddress
	 * @param tableName
	 *            is uniqule
	 * @return
	 */
	public static LinkedHashMap<String, String> getTableFieldTypeMap(
			String dbAddress, String tableName) {

		Connection conn = null;
		Statement st = null;
		ResultSet rs = null;

		try {
			conn = H2ConnectionPool.getPool(dbAddress).getConnection();

			st = conn.createStatement();
			String querySql = "select tablefields from LP_META"
					+ " where tableName = '" + tableName + "'";
			rs = st.executeQuery(querySql);

			String fields = null;
			while (rs.next()) {
				fields = rs.getString("tablefields");
				// only handle the first line, assume only have one line
				break;
			}

			String[] blocks = fields.split(",");
			LinkedHashMap<String, String> fieldTypes = new LinkedHashMap<>();
			for (String field : blocks) {
				String[] array = field.split("=");
				String name = array[0];
				String type = array[1];
				fieldTypes.put(name, type);
			}
			return fieldTypes;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			closeAutoCloseable(rs, "close database result set failed");
			closeAutoCloseable(st, "close database statement failed");
			closeAutoCloseable(conn, "close database connection failed");
		}
	}

	/**
	 * close the resource
	 * @param autoCloseable
	 * @param errorMsg
	 */
	private static void closeAutoCloseable(AutoCloseable autoCloseable,
			String errorMsg) {
		if (null != autoCloseable) {
			try {
				autoCloseable.close();
			} catch (Exception e) {
				log.error(errorMsg, e);
			}
		}
	}

	/**
	 * 
	 * @param tableName
	 * @param length
	 * @return
	 */
	private static String generateInsertSql(String tableName,
			LinkedHashMap<String, String> fieldTypeMap) {
		StringBuffer buffer = new StringBuffer("insert into ")
				.append(tableName);

		StringBuffer names = new StringBuffer();
		StringBuffer values = new StringBuffer();

		for (Entry<String, String> entry : fieldTypeMap.entrySet()) {
			names.append("," + entry.getKey());
			values.append(",?");

		}

		buffer.append("(").append(names.substring(1)).append(") ")
				.append("values(").append(values.substring(1)).append(")");
		return buffer.toString();
	}

	public static void main(String[] args) {
		LinkedHashMap<String, String> fieldTypeMap = new LinkedHashMap<>();
		fieldTypeMap.put("a", "aa");
		fieldTypeMap.put("b", "bb");
		System.out.println(generateInsertSql("test1", fieldTypeMap));
	}

	public static void executeDbStatement(String dbAddress, String sql,
			String errorMsg) {

		Connection conn = null;
		Statement st = null;

		try {
			conn = H2ConnectionPool.getPool(dbAddress).getConnection();
			st = conn.createStatement();
			st.execute(sql);
		} catch (SQLException e) {
			throw new RuntimeException(errorMsg, e);
		} finally {
			closeAutoCloseable(st, "close database statement failed");
			closeAutoCloseable(conn, "close database connection failed");
		}
	}

	private static final EmittingLogger log = new EmittingLogger(
			H2InMemoryDbUtil.class);
}
