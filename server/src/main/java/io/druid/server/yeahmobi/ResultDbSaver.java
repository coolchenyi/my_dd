package io.druid.server.yeahmobi;

/**
 * Created by oscar.gao on 8/4/14.
 */

import io.druid.query.Query;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.timeseries.TimeseriesQuery;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.http.HttpServletRequest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.metamx.common.guava.Sequence;

/**
 * used to do Landing page
 * 
 */
public class ResultDbSaver {

	private HttpServletRequest request;
	private String tableName;
	private String dbAddress;
	private Sequence<?> druidResult;
	private ObjectMapper objectMapper;
	private ObjectWriter jsonWriter;
	private Query query;

	private boolean isGroupBy;
	private boolean isTimeseries;

	/**
	 * if need landing page process only group by and time series do landing
	 * page
	 * 
	 * @return
	 */
	public static boolean isNeedSaveToDb(HttpServletRequest request, Query query) {
		String tableName = request.getParameter("tablename");
		String dbAddress = request.getParameter("db");
		boolean notEmpty = request.getParameterMap().containsKey("saveToDb")
				&& (tableName != null && !tableName.isEmpty()
						&& dbAddress != null && !dbAddress.isEmpty());
		if (notEmpty) {
			// only group by and time series type need special process.
			return (query instanceof GroupByQuery || query instanceof TimeseriesQuery);
		} else {
			return false;
		}
	}

	/**
	 * constructor
	 * 
	 * @param request
	 * @param druidResult
	 * @param objectMapper
	 * @param jsonWriter
	 * @param query
	 */
	public ResultDbSaver(HttpServletRequest request, Sequence<?> druidResult,
			ObjectMapper objectMapper, ObjectWriter jsonWriter, Query query) {

		this.request = request;
		this.tableName = request.getParameter("tablename");
		this.dbAddress = request.getParameter("db");
		this.druidResult = druidResult;
		this.objectMapper = objectMapper;
		this.jsonWriter = jsonWriter;
		this.query = query;

		if (this.query instanceof GroupByQuery) {
			isGroupBy = true;
		}

		if (this.query instanceof TimeseriesQuery) {
			isTimeseries = true;
		}
	}

	/**
	 * insert the druid result into in-memory db<br>
	 * instead of return the result to real-query node directly
	 * 
	 * @throws IOException
	 */
	public void insertData() throws IOException {
		byte[] bytes = jsonWriter.writeValueAsBytes(druidResult);
		LinkedHashMap<String, String> fieldMap = H2InMemoryDbUtil
				.getTableFieldTypeMap(dbAddress, tableName);
		List<List<Object>> rows = parseData(objectMapper, bytes, fieldMap);
		H2InMemoryDbUtil.insertData(dbAddress, tableName, rows, fieldMap);
	}

	/**
	 * parse the following json <br>
	 * [ { <br>
	 * "version" : "v1", "timestamp" : <br>
	 * "2014-07-17T03:00:00.000Z", <br>
	 * "event" : { <br>
	 * "page" : "!!", <br>
	 * "edit_count" : 1, <br>
	 * "rows" : 2 <br>
	 * } <br>
	 * }, ... <br>
	 * ] <br>
	 * Note: group by is "event" and time series is "result"<br>
	 * 
	 * 
	 * @throws IOException
	 */
	public static List<List<Object>> parseData(ObjectMapper objectMapper,
			byte[] bytes, LinkedHashMap<String, String> fieldTypeMap)
			throws IOException {

		List<List<Object>> rows = new LinkedList<>();
		ArrayNode root = (ArrayNode) objectMapper.readTree(bytes);
		for (JsonNode o : root) {

			// read the data to rowMap
			Map<String, Object> rowMap = new HashMap<>();
			Iterator<Entry<String, JsonNode>> iter = o.fields();
			while (iter.hasNext()) {
				Entry<String, JsonNode> outerObject = iter.next();
				if (outerObject.getKey().equals("timestamp")) {
					rowMap.put("timestamp", outerObject.getValue());
				} else if (outerObject.getKey().equals("event")
						|| outerObject.getKey().equals("result")) {
					Iterator<Entry<String, JsonNode>> innerIter = outerObject
							.getValue().fields();
					while (innerIter.hasNext()) {
						Entry<String, JsonNode> innerObject = innerIter.next();
						rowMap.put(innerObject.getKey(), innerObject.getValue());
					}

				} else {
					// ignore
				}
			}

			// parse the rowMap
			List<Object> rowList = new LinkedList<>();
			for (Entry<String, String> entry : fieldTypeMap.entrySet()) {
				String fieldName = entry.getKey();
				String type = entry.getValue();
				Object value = rowMap.get(fieldName);
				Object convertedValue = convertAccordingType(type, value);
				rowList.add(convertedValue);
			}
			rows.add(rowList);
		}
		return rows;
	}

	/**
	 * should convert the druid data<br>
	 * for example: the device_id should be integer<br>
	 * but sometimes return value is string "1"<br>
	 * should delete the ", and parse to integer
	 * 
	 * @param type
	 * @param value
	 * @return
	 */
	private static Object convertAccordingType(String type, Object value) {
		
		if (type.equals(H2FieldType.VARCHAR.toString())) {
			if(value instanceof JsonNode){
				return ((JsonNode)value).asText();
			}else{
				// is null or other value
				return "";
			}
		} else if (type.equals(H2FieldType.DECIMAL.toString())) {
			if (value instanceof TextNode) {
				// delete the begin and end "
				String numStr = ((TextNode)value).asText().replaceAll("\"", "");
				return new BigDecimal(numStr);
			} else if( value instanceof NumericNode ) {
				return new BigDecimal(((NumericNode)value).asText());
			} else{
				// is null or other value
				return new BigDecimal(0);
			}
		} else if (type.equals(H2FieldType.BIGINT.toString())) {
			if (value instanceof TextNode) {
				// delete the begin and end "
				String numStr = ((TextNode)value).asText().replaceAll("\"", "");
				return Long.parseLong(numStr);
			} else if(value instanceof NumericNode) {
				return new Long(((NumericNode)value).asText());
			} else{
				// is null or other value
				return new Long(0);
			}
		} else {
			throw new RuntimeException("can't handle type " + type);
		}
	}

	/**
	 * the table name that store the druid result
	 * 
	 * @return
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * test
	 * 
	 * @param args
	 * @throws UnsupportedEncodingException
	 * @throws IOException
	 */
	public static void main(String[] args) throws UnsupportedEncodingException,
			IOException {

		LinkedHashMap<String, String> fieldTypeMap = new LinkedHashMap<>();
		fieldTypeMap.put("timestamp", "VARCHAR");
		fieldTypeMap.put("page", "VARCHAR");
		fieldTypeMap.put("edit_count", "DECIMAL");
		fieldTypeMap.put("rows", "BIGINT");

		parseData(
				new ObjectMapper(),
				"[		{		  \"version\" : \"v1\",		  \"timestamp\" : \"2014-07-17T03:00:00.000Z\",		  \"event\" : {			\"page\" : \"!!\",			\"edit_count\" : 1.15926,			\"rows\" : 1		  }		}	]"
						.getBytes("UTF-8"), fieldTypeMap);
	}
}
