package com.yeahmobi.druid;
import java.util.List;

public class Spec
{
	private List<String> dimentions;
	private List<Aggregator> aggregators;

	public Spec(List<String> dimentions, List<Aggregator> aggregators){
		this.dimentions = dimentions;
		this.aggregators = aggregators;
	}
	
	public List<String> getDimentions()
	{
		return dimentions;
	}

	public void setDimentions(List<String> dimentions)
	{
		this.dimentions = dimentions;
	}

	public List<Aggregator> getAggregators()
	{
		return aggregators;
	}

	public void setAggregators(List<Aggregator> aggregators)
	{
		this.aggregators = aggregators;
	}


	public static class Aggregator
	{
		private String name;
		private AggregatorType type;

		public Aggregator(String name, AggregatorType type){
			this.name = name;
			this.type = type;
		}
		
		public String getName()
		{
			return name;
		}

		public void setName(String name)
		{
			this.name = name;
		}

		public AggregatorType getType()
		{
			return type;
		}

		public void setType(AggregatorType type)
		{
			this.type = type;
		}
	}
	
	
	public static enum AggregatorType
	{
		longsum,
		doublesum
	}
}
