package com.yeahmobi.druid;

import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMerger;
import io.druid.segment.QueryableIndex;
import io.druid.segment.incremental.IncrementalIndex;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;

public class Encode
{
	private static int bulkSize = 300000;

	public static void encode(String flatFilePath, String decodedDir, String datasourceName) throws Exception
	{

		long firstTimeStamp = 0L;
		int maxBulk = 0;
		try (BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(flatFilePath), Charsets.UTF_8)))
		{
			String line = in.readLine();
			TypeReference<ResultRow> typeReference = new TypeReference<ResultRow>()
			{
			};
			ResultRow row = objectMapper.readValue(line, typeReference);
			firstTimeStamp = DateTime.parse(row.getTimestamp()).getMillis();
			AggregatorFactory[] metrics = Specs.getMetrics(datasourceName).toArray(new AggregatorFactory[0]);

			IncrementalIndex index = new IncrementalIndex(firstTimeStamp,
			    QueryGranularity.MINUTE,
			    metrics);

			index.add(Results.toInputRow(row));

			int currentLine = 1;

			while ((line = in.readLine()) != null)
			{
				line = line.trim();
				if (line.length() > 0)
				{
					currentLine++;
				} else
				{
					continue;
				}

				row = objectMapper.readValue(line, typeReference);
				index.add(Results.toInputRow(row));

				if (currentLine == bulkSize)
				{
					String filePath = decodedDir + "/" + String.valueOf(maxBulk);
					File f = new File(filePath);
					f.mkdirs();

					IndexMerger.persist(index, f);
					index = new IncrementalIndex(firstTimeStamp,
					    QueryGranularity.MINUTE,
					    metrics);
					currentLine = 0;
					maxBulk++;
				}
			}

			if (currentLine > 0)
			{
				String filePath = decodedDir + "/" + String.valueOf(maxBulk);
				File f = new File(filePath);
				f.mkdirs();
				IndexMerger.persist(index, f);
			}

			if (currentLine == 0)
			{
				maxBulk--;
			}
		}

		List<QueryableIndex> indexes = new ArrayList<>();
		for (int i = 0; i <= maxBulk; ++i)
		{
			indexes.add(IndexIO.loadIndex(new File(decodedDir + "/" + String.valueOf(i))));
		}

		File mergedTarget = new File(decodedDir);

		IndexMerger.mergeQueryableIndex(
		    indexes,
		    Specs.getMetricArray(datasourceName),
		    mergedTarget
		    );

		for (int i = 0; i <= maxBulk; ++i)
		{
			FileUtils.deleteDirectory(new File(decodedDir + "/" + String.valueOf(i)));
		}
	}

	public static void main(String[] args)
	{
		if (!isArgumentValid(args))
		{
			return;
		}

		try
		{
			encode(args[0], args[1], args[2]);

		} catch (Exception e)
		{
			System.out.println("Error occured");
			e.printStackTrace();
		}
	}

	private static ObjectMapper objectMapper = new ObjectMapper();

	private static boolean isArgumentValid(String[] args)
	{
		if (args.length != 3)
		{
			System.out.println("argument is wrong!");
			System.out.println("Usage is: ");
			System.out.println("  Decode <segment_path> <output_path> <datasource_name>");
			System.out.println("  datasource name have yeah_mobi");

			return false;
		} else
		{
			return true;
		}
	}
}
