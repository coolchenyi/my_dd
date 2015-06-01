package com.yeahmobi.druid;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.IndexIO;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.data.IndexedInts;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;
import java.util.Map;

import org.joda.time.Interval;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.parsers.CloseableIterator;

public class Decode
{

	private static ObjectMapper objectMapper = new ObjectMapper();

	private static long END_TIME = 4099651200000L;

	public static void decode(String inDir, String outFile, final String datasourceName) throws Exception
	{

		System.out.println("begin decoding segment " + inDir);
		File segmentDir = new File(inDir);
		Segment segment = new QueryableIndexSegment(segmentDir.getName(),
		    IndexIO.loadIndex(segmentDir));

		final Sequence<Cursor> cursors = segment.asStorageAdapter()
		    .makeCursors(null, new Interval(0L, END_TIME),
		        QueryGranularity.NONE);

		Sequence<Row> seq = Sequences.concat(Sequences.map(cursors,
		    new Function<Cursor, Sequence<Row>>()
		    {

			    @Override
			    public Sequence<Row> apply(final Cursor cursor)
			    {
				    final List<DimensionSpec> dimSpecs = Specs.getDimentions(datasourceName);
				    final List<AggregatorFactory> aggs = Specs.getMetrics(datasourceName);
				    final List<FloatColumnSelector> aggSelectors = Lists.newArrayList();
				    final List<DimensionSelector> selectors = Lists.newArrayList();

				    for (DimensionSpec dimSpec : dimSpecs)
				    {
					    DimensionSelector selector = cursor
					        .makeDimensionSelector(dimSpec
					            .getDimension());

					    selectors.add(selector);
				    }

				    for (AggregatorFactory agg : aggs)
				    {
					    aggSelectors.add(cursor.makeFloatColumnSelector(agg.getName()));
				    }

				    return new BaseSequence<Row, RowIterator>(
				        new BaseSequence.IteratorMaker<Row, RowIterator>()
				        {
					        @Override
					        public RowIterator make()
					        {
						        return new RowIterator(cursor, selectors, dimSpecs, aggs, aggSelectors);
					        }

					        @Override
					        public void cleanup(RowIterator iterFromMake)
					        {
						        Closeables.closeQuietly(iterFromMake);
					        }
				        }
				    );
			    }
		    }));

		OutputStream out = new BufferedOutputStream(new FileOutputStream(outFile));
		Writer writer = new OutputStreamWriter(out, Charsets.UTF_8);

		Accumulator<Writer, Row> accumulator = new Accumulator<Writer, Row>()
		{
			@Override
			public Writer accumulate(Writer out, Row row)
			{
				try
				{
					String rowStr = objectMapper.writeValueAsString(Results.toResultRow((MapBasedRow) row));
					out.write(rowStr + "\n");

				} catch (IOException e)
				{
					throw new RuntimeException("write to file error");
				}
				return out;
			}
		};

		seq.accumulate(writer, accumulator);
		writer.close();
		out.close();
		segment.close();
		System.out.println("end decoding segment " + inDir);
	}

	private static class RowIterator implements CloseableIterator<Row>
	{
		private Cursor cursor;
		private List<DimensionSpec> dimSpecs;
		private List<DimensionSelector> selectors;
		private List<AggregatorFactory> aggs;
		private List<FloatColumnSelector> aggSelectors;

		public RowIterator(Cursor cursor, List<DimensionSelector> selectors, List<DimensionSpec> dimSpecs, List<AggregatorFactory> aggs,
		    List<FloatColumnSelector> aggSelectors)
		{
			this.cursor = cursor;
			this.dimSpecs = dimSpecs;
			this.selectors = selectors;
			this.aggs = aggs;
			this.aggSelectors = aggSelectors;
		}

		@Override
		public boolean hasNext()
		{
			boolean isDone = cursor.isDone();
			return !isDone;
		}

		@Override
		public Row next()
		{

			Map<String, Object> theEvent = Maps.newLinkedHashMap();
			for (int i = 0; i < selectors.size(); ++i)
			{

				DimensionSelector dimSelector = selectors.get(i);
				if (dimSelector == null)
				{
					continue;
				}

				IndexedInts row = dimSelector.getRow();
				if (row.size() > 1)
				{
					throw new RuntimeException("have multiple value in column");
				}

				String value = "";
				if (row.size() > 0)
				{
					int dimIndex = row.get(0);
					value = dimSelector.lookupName(dimIndex);
				}

				theEvent.put(dimSpecs.get(i).getDimension(), value);
			}

			for (int i = 0; i < aggs.size(); ++i)
			{
				if (aggs.get(i) instanceof LongSumAggregatorFactory)
				{
					theEvent.put(aggs.get(i).getName(), new Float(aggSelectors.get(i).get()).longValue());
				} else if (aggs.get(i) instanceof DoubleSumAggregatorFactory)
				{
					theEvent.put(aggs.get(i).getName(), aggSelectors.get(i).get());
				} else
				{
					throw new RuntimeException("wrong type " + aggs.get(i));
				}
			}

			cursor.advance();

			return new MapBasedRow(cursor.getTime(), theEvent);
		}

		@Override
		public void remove()
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public void close() throws IOException
		{
		}

	}

	private static void checkArgumentValid(String[] args)
	{
		if (args.length != 3)
		{
			System.out.println("argument is wrong!");
			System.out.println("Usage is: ");
			System.out.println("  Decode <segment_path> <output_path> <datasource_name>");
			throw new RuntimeException("");
		}
	}

	public static void main(String[] args)
	{
		try
		{
			checkArgumentValid(args);
			decode(args[0], args[1], args[2]);
		} catch (Exception e)
		{
			System.out.println("Error occured");
			e.printStackTrace();
		}
	}
}
