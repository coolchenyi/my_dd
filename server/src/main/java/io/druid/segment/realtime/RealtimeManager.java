/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment.realtime;

import io.druid.data.input.Firehose;
import io.druid.data.input.InputRow;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.NoopQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.SegmentDescriptor;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.realtime.plumber.Plumber;
import io.druid.segment.realtime.plumber.RealtimePlumber;

import java.io.Closeable;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.google.inject.Inject;
import com.metamx.common.exception.FormattedException;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.emitter.EmittingLogger;

/**
 */
public class RealtimeManager implements QuerySegmentWalker
{
  private static final EmittingLogger log = new EmittingLogger(RealtimeManager.class);
  private static String LOG_DIR = "/tmpdata/druid_consumer_log";
  private final List<FireDepartment> fireDepartments;
  private final QueryRunnerFactoryConglomerate conglomerate;

  private final Map<String, FireChief> chiefs;

  @Inject
  public RealtimeManager(
      List<FireDepartment> fireDepartments,
      QueryRunnerFactoryConglomerate conglomerate
  )
  {
    this.fireDepartments = fireDepartments;
    this.conglomerate = conglomerate;

    this.chiefs = Maps.newHashMap();

	String logDir = System.getProperty("druid.consumer.log.path");
	if (!Strings.isNullOrEmpty(logDir)) {
		LOG_DIR = logDir;
	}
  }

  @LifecycleStart
  public void start() throws IOException
  {
    for (final FireDepartment fireDepartment : fireDepartments) {
      DataSchema schema = fireDepartment.getDataSchema();

      final FireChief chief = new FireChief(fireDepartment);
      chiefs.put(schema.getDataSource(), chief);

      chief.setName(String.format("chief-%s", schema.getDataSource()));
      chief.setDaemon(true);
      chief.init();
      chief.start();
      
      RealtimeStopManager.getFireChiefMap().put(schema.getDataSource(), chief);
      RealtimeStopManager.getFireChiefAlreadyStopMap().put(schema.getDataSource(), false);
    }
    
    RealtimeStopManager.startListener();
  }

  @LifecycleStop
  public void stop()
  {
    for (FireChief chief : chiefs.values()) {
      Closeables.closeQuietly(chief);
    }
  }

  public FireDepartmentMetrics getMetrics(String datasource)
  {
    FireChief chief = chiefs.get(datasource);
    if (chief == null) {
      return null;
    }
    return chief.getMetrics();
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
  {
    final FireChief chief = chiefs.get(getDataSourceName(query));

    return chief == null ? new NoopQueryRunner<T>() : chief.getQueryRunner(query);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    final FireChief chief = chiefs.get(getDataSourceName(query));

    return chief == null ? new NoopQueryRunner<T>() : chief.getQueryRunner(query);
  }

  private <T> String getDataSourceName(Query<T> query)
  {
    return Iterables.getOnlyElement(query.getDataSource().getNames());
  }


  class FireChief extends Thread implements Closeable
  {
    private final FireDepartment fireDepartment;
    private final FireDepartmentMetrics metrics;

    private volatile RealtimeTuningConfig config = null;
    private volatile Firehose firehose = null;
    private volatile Plumber plumber = null;
    private volatile boolean normalExit = true;

    public FireChief(
        FireDepartment fireDepartment
    )
    {
      this.fireDepartment = fireDepartment;

      this.metrics = fireDepartment.getMetrics();
    }

    public void init() throws IOException
    {
      config = fireDepartment.getTuningConfig();

      synchronized (this) {
        try {
          log.info("Calling the FireDepartment and getting a Firehose.");
          firehose = fireDepartment.connect();
          log.info("Firehose acquired!");
          log.info("Someone get us a plumber!");
          plumber = fireDepartment.findPlumber();
          log.info("We have our plumber!");
        }
        catch (IOException e) {
          throw Throwables.propagate(e);
        }
      }
    }

    public synchronized void persistentForSafeClose(){
      try {
        if(firehose != null){
          firehose.close();        	
          RealtimeStopManager.sendToClient(String.format("data source %s close fire hose successfully", fireDepartment.getDataSchema().getDataSource()));
        }
      } catch (IOException e) {
    	log.warn("close firehose error", e);
    	RealtimeStopManager.sendToClient(String.format("data source %s close fire hose failed: %s", fireDepartment.getDataSchema().getDataSource(), e));
      }
      firehose = null;
      if(plumber != null){
        if(plumber instanceof RealtimePlumber){
        	((RealtimePlumber)plumber).persistentForSafeClose();
        }
        plumber = null;
     }else{
		if(RealtimeStopManager.getFireChiefAlreadyStopMap().containsKey(fireDepartment.getDataSchema().getDataSource())){
			RealtimeStopManager.sendToClient(String.format("data source %s plumber is null, no need to shut down", fireDepartment.getDataSchema().getDataSource()));
			RealtimeStopManager.getFireChiefAlreadyStopMap().put(fireDepartment.getDataSchema().getDataSource(), true);
		}
     }
    }

    public FireDepartmentMetrics getMetrics()
    {
      return metrics;
    }

    @Override
    public void run()
    {
      verifyState();

      final Period intermediatePersistPeriod = config.getIntermediatePersistPeriod();
      try {
        plumber.startJob();

        long nextFlush = new DateTime().plus(intermediatePersistPeriod).getMillis();
        while ((!RealtimeStopManager.isSetStopFlag()) && firehose.hasMore()) {
        	
          if(RealtimeStopManager.isSetStopFlag()){
            break;
          }
          
          final InputRow inputRow;
          try {
            try {
              inputRow = firehose.nextRow();
            }
            catch (Exception e) {
              log.debug(e, "thrown away line due to exception, considering unparseable");
              log.warn(e, "error: can't parse one message");
              
              
      		  long timestamp = System.currentTimeMillis();
    		  String date = new DateTime(timestamp).toString("yyyy-MM-dd-HH");
    		  date = date + "-unparsed.log";
    		  String fileName = String.format("%s_%s", fireDepartment.getDataSchema().getDataSource(), date);
    		  String filePath = LOG_DIR + "/" + fileName;
    		  try (FileWriter writtter = new FileWriter(filePath, true)) {
    			writtter.write(e.toString() + "\n");
    		  } catch (Exception ee) {
    			log.error("write unparsed consumer log error");
    		  }

              metrics.incrementUnparseable();
              continue;
            }

            int currCount = plumber.add(inputRow);
            if (currCount == -1) {
              metrics.incrementThrownAway();
              log.debug("Throwing away event[%s]", inputRow);

              if (System.currentTimeMillis() > nextFlush) {
                plumber.persist(firehose.commit());
                nextFlush = new DateTime().plus(intermediatePersistPeriod).getMillis();
              }

              continue;
            }
            if (currCount >= config.getMaxRowsInMemory() || System.currentTimeMillis() > nextFlush) {
              plumber.persist(firehose.commit());
              nextFlush = new DateTime().plus(intermediatePersistPeriod).getMillis();
            }
            metrics.incrementProcessed();
          }
          catch (FormattedException e) {
            log.info(e, "unparseable line: %s", e.getDetails());
            metrics.incrementUnparseable();
            continue;
          }
        }
      }
      catch (RuntimeException e) {
        log.makeAlert(
            e,
            "RuntimeException aborted realtime processing[%s]",
            fireDepartment.getDataSchema().getDataSource()
        ).emit();
        normalExit = false;
        throw e;
      }
      catch (Error e) {
        log.makeAlert(e, "Exception aborted realtime processing[%s]", fireDepartment.getDataSchema().getDataSource())
           .emit();
        normalExit = false;
        throw e;
      }
      finally {
        Closeables.closeQuietly(firehose);
        if (!normalExit) {
        	log.error("stop real time in abnormal status");
        }
      }
    }

    private void verifyState()
    {
      Preconditions.checkNotNull(config, "config is null, init() must be called first.");
      Preconditions.checkNotNull(firehose, "firehose is null, init() must be called first.");
      Preconditions.checkNotNull(plumber, "plumber is null, init() must be called first.");

      log.info("FireChief[%s] state ok.", fireDepartment.getDataSchema().getDataSource());
    }

    public <T> QueryRunner<T> getQueryRunner(Query<T> query)
    {
      QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
      QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();

      return new FinalizeResultsQueryRunner<T>(plumber.getQueryRunner(query), toolChest);
    }

    public void close() throws IOException
    {
      synchronized (this) {
        if (firehose != null) {
          normalExit = false;
          firehose.close();
        }
      }
    }
  }
}
