package com.yeahmobi.druid;
public class TestIncodeDecode
{
	public static void main(String[] args)
	{
		try
		{
			Encode.encode("e:\\tmp\\raw.txt", "e:\\tmp\\encoded",
			    "yeah_mobi");
			Decode.decode("e:\\tmp\\encoded", "e:\\tmp\\decoded.txt",
			    "yeah_mobi");
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}
