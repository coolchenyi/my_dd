package io.druid.segment.realtime.plumber;

import io.druid.data.input.MapBasedInputRow;

import java.util.List;

public class YeahMobiDuplicateRaw implements RowDuplicateRaw
{

	@Override
	public boolean isDealDuplicate(MapBasedInputRow row)
	{
		List<String> statusList = row.getDimension("status");
		List<String> log_tye = row.getDimension("log_tye");
		List<String> tranId = row
		    .getDimension("transaction_id");

		if (statusList != null && statusList.size() != 0
		    && "Confirmed".equalsIgnoreCase(statusList.get(0))
		    && log_tye != null && log_tye.size() != 0
		    && "1".equalsIgnoreCase(log_tye.get(0)) && tranId != null && log_tye.size() != 0)
		{
			return true;
		} else
		{
			return false;
		}
	}

	@Override
	public String getKey(MapBasedInputRow row)
	{
		List<String> tranId = row
		    .getDimension("transaction_id");
		String transactionId = "";
		if (tranId != null && tranId.size() != 0)
		{
			transactionId = tranId.get(0);
		}
		return transactionId;
	}
}
