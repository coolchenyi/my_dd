package com.yeahmobi.druid;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class Specs
{
	private static Map<String, Spec> SPECS = new HashMap<>();

	static
	{

		SPECS.put("yeah_mobi", new Spec(ImmutableList.<String>
		    of(
		        "aff_id", "aff_manager", "aff_sub1", "aff_sub2", "aff_sub3",
		        "aff_sub4", "aff_sub5", "aff_sub6", "aff_sub7", "aff_sub8",
		        "adv_id", "adv_manager", "adv_sub1", "adv_sub2", "adv_sub3",
		        "adv_sub4", "adv_sub5", "adv_sub6", "adv_sub7", "adv_sub8",
		        "offer_id", "currency", "rpa", "cpa", "ref_track",
		        "ref_track_site", "ref_conv_track", "click_ip", "conv_ip",
		        "transaction_id", "click_time", "conv_time", "time_diff",
		        "user_agent", "browser", "device_brand", "device_model",
		        "device_os", "device_type", "country", "time_stamp", "log_tye",
		        "visitor_id", "x_forwarded_for", "state", "city", "isp",
		        "mobile_brand", "platform_id", "screen_width", "screen_height",
		        "type_id", "conversions", "track_type", "session_id",
		        "visitor_node_id", "expiration_date", "is_unique_click", "gcid",
		        "gcname", "browser_name", "device_brand_name", "device_model_name",
		        "platform_name", "device_type_name", "os_ver_name", "os_ver",
		        "datasource"), ImmutableList.<Spec.Aggregator> of(new Spec.Aggregator("click",
		    Spec.AggregatorType.longsum),
		    new Spec.Aggregator("unique_click", Spec.AggregatorType.longsum),
		    new Spec.Aggregator("conversion", Spec.AggregatorType.longsum),
		    new Spec.Aggregator("cost", Spec.AggregatorType.doublesum),
		    new Spec.Aggregator("revenue", Spec.AggregatorType.doublesum)
		    )));
	}

	public static Spec getSpec(String datasourceName)
	{
		return SPECS.get(datasourceName);
	}

	public static List<AggregatorFactory> getMetrics(String datasourceName)
	{
		return Lists.transform(getSpec(datasourceName).getAggregators(),
		    new Function<Spec.Aggregator, AggregatorFactory>()
		    {
			    @Override
			    public AggregatorFactory apply(Spec.Aggregator input)
			    {
				    if (input.getType() == Spec.AggregatorType.longsum)
				    {
					    return new LongSumAggregatorFactory(input.getName(), input.getName());
				    } else if (input.getType() == Spec.AggregatorType.doublesum)
				    {
					    return new DoubleSumAggregatorFactory(input.getName(), input.getName());
				    } else
				    {
					    throw new RuntimeException("wrong type " + input.getType());
				    }
			    }
		    });
	}
	
	public static AggregatorFactory[] getMetricArray(String datasourceName)
	{
		return getMetrics(datasourceName).toArray(new AggregatorFactory[0]);
	}
	
	public static List<DimensionSpec> getDimentions(String datasourceName){
		return Lists.transform(getSpec(datasourceName).getDimentions(), new Function<String, DimensionSpec>(){

			@Override
      public DimensionSpec apply(String input)
      {
				return new DefaultDimensionSpec(input, input);
      }
		});
	}
}
