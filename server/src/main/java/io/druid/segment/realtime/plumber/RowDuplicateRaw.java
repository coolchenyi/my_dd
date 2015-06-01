package io.druid.segment.realtime.plumber;

import io.druid.data.input.MapBasedInputRow;

public interface RowDuplicateRaw
{
	boolean isDealDuplicate(MapBasedInputRow row);

	String getKey(MapBasedInputRow row);
}
