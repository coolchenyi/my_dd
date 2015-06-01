package com.yeahmobi.druid;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.MapBasedRow;

import java.util.List;

import org.joda.time.DateTime;

import com.google.common.collect.Lists;

public class Results
{
	public static ResultRow toResultRow(MapBasedRow row)
	{
		ResultRow ret = new ResultRow();
		ret.setResult(row.getEvent());
		String timestamp = row.getTimestamp().toString();
		ret.setTimestamp(timestamp);
		return ret;
	}

	public static MapBasedInputRow toInputRow(ResultRow row)
	{
		List<String> dimentions = Lists.newArrayList(row.getResult().keySet());
		long time = DateTime.parse(row.getTimestamp()).getMillis();
		
		return new MapBasedInputRow(time, dimentions, row.getResult());
	}
}
