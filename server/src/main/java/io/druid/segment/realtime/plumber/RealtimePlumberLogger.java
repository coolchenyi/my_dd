package io.druid.segment.realtime.plumber;

import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.joda.time.DateTime;

import com.google.common.base.Strings;
import com.google.common.io.Files;
import com.metamx.common.Granularity;
import com.metamx.emitter.EmittingLogger;

/**
 */
public class RealtimePlumberLogger {

	private static Map<String, RealtimePlumberLogger> loggers = new ConcurrentHashMap<>();

	private static final EmittingLogger log = new EmittingLogger(
			RealtimePlumberLogger.class);
	private static final String CFG_PATH = "/dianyi/druid_config/enable_druid_consumer_log";
	private static String LOG_DIR = "/tmpdata/druid_consumer_log";
	private static String WARN_DIR = "/tmpdata/for_warning/not_send";
	private static String DELAY_WARN_CFG = "/dianyi/druid_config/delay_warn.properties";

	private String dataSource;

	boolean isFirstRow = true;
	private Granularity granularity;

	private Map<DateTime, Integer> delayMap = new HashMap<>();
	private int currentHour = 0;
	private volatile int delay_threshold = 60 * 1000;
	private volatile int delay_warn_num = 1000;
	private volatile int delay_warn_again_num = 10000;

	private Map<Long, Boolean> isLogMap = new HashMap<>();
	private Map<Long, BufferedWriter> logWriterMap = new HashMap<>();

	public static RealtimePlumberLogger newLogger(String dataSource,
			Granularity granularity) {
		RealtimePlumberLogger logger = new RealtimePlumberLogger(dataSource,
				granularity);
		loggers.put(dataSource, logger);
		return logger;
	}

	private RealtimePlumberLogger(String dataSource, Granularity granularity) {
		this.dataSource = dataSource;
		this.granularity = granularity;

		String logDir = System.getProperty("druid.consumer.log.path");
		if (!Strings.isNullOrEmpty(logDir)) {
			LOG_DIR = logDir;
		}

		if (!new File(LOG_DIR).exists()) {
			new File(LOG_DIR).mkdirs();
		}
	}

	private boolean isLog() {
		File file = new File(CFG_PATH);
		if (file.exists() && file.isFile()) {
			return true;
		} else {
			return false;
		}
	}

	public void addLog(MapBasedInputRow row, boolean isThrown,
			long currentCount, int partition) {
		long curr = System.currentTimeMillis();
		long diff = curr - row.getTimestampFromEpoch();

		DateTime dateTime = new DateTime(curr);
		reloadDelayCfg(dateTime);

		DateTime startTime = dateTime.withTimeAtStartOfDay();
		if (diff >= delay_threshold) {
			if (null == delayMap.get(startTime)) {
				String day = startTime.toString("yyyy-MM-dd");
				String warnFileName = String
						.format("[%s] have delay message, delay beyond %s, curr delay number is 1",
								day, delay_threshold);
				try {
					Files.touch(new File(WARN_DIR + "/" + warnFileName));
				} catch (Exception e) {
					log.error("touch warn file error " + WARN_DIR + "/"
							+ warnFileName);
				}
				delayMap.put(startTime, 1);
			} else {
				Integer num = delayMap.get(startTime);
				delayMap.put(startTime, ++num);
				if (num == delay_warn_num) {
					String day = startTime.toString("yyyy-MM-dd");
					String warnFileName = String
							.format("[%s] have delay message, delay beyond %s, curr delay number is %s",
									day, delay_threshold, num);
					try {
						Files.touch(new File(WARN_DIR + "/" + warnFileName));
					} catch (Exception e) {
						log.error("touch warn file error " + WARN_DIR + "/"
								+ warnFileName);
					}
				}
				if (num == delay_warn_again_num) {
					String day = startTime.toString("yyyy-MM-dd");
					String warnFileName = String
							.format("[%s] have delay message, delay beyond %s, curr delay number is %s",
									day, delay_threshold, num);
					try {
						Files.touch(new File(WARN_DIR + "/" + warnFileName));
					} catch (Exception e) {
						log.error("touch warn file error " + WARN_DIR + "/"
								+ warnFileName);
					}
				}
			}
		}

		if (isThrown) {
			addLogForThrown(row, currentCount, partition);
		} else {
			addLogForNomal(row, currentCount, partition);
		}
	}

	private void addLogForThrown(MapBasedInputRow row, long currentCount,
			int partition) {
		long timestamp = row.getTimestampFromEpoch();
		long truncatedTime = granularity.truncate(new DateTime(timestamp))
				.getMillis();
		String date = new DateTime(truncatedTime).toString("yyyy-MM-dd-HH");
		date = date + "-throw.log";
		String fileName = String.format("%s_%s", dataSource, date);

		String filePath = LOG_DIR + "/" + fileName;

		try (FileWriter writtter = new FileWriter(filePath, true)) {

			String msg = String.format(
					"curr:%s|partition:%s|count:%s|msg:%s\n", new DateTime(),
					partition, currentCount, row.getEvent());
			writtter.write(msg);
		} catch (Exception e) {
			log.error("write throwned consumer log error");
		}
	}

	private void reloadDelayCfg(DateTime dateTime) {

		if (dateTime.getHourOfDay() != currentHour) {

			try (InputStream fileInputStream = new FileInputStream(
					DELAY_WARN_CFG)) {
				Properties p = new Properties();
				p.load(new FileInputStream(DELAY_WARN_CFG));
				delay_threshold = Integer.parseInt(p
						.getProperty("delay_threshold"));
				delay_warn_num = Integer.parseInt(p
						.getProperty("delay_warn_num"));
				delay_warn_again_num = Integer.parseInt(p
						.getProperty("delay_warn_again_num"));

			} catch (Exception e) {
				log.error("load delay cfg error, path is " + DELAY_WARN_CFG);
			}

			currentHour = dateTime.getHourOfDay();
		}
	}

	private void addLogForNomal(MapBasedInputRow row, long currentCount,
			int partition) {

		long timestamp = row.getTimestampFromEpoch();

		long truncatedTime = granularity.truncate(new DateTime(timestamp))
				.getMillis();
		boolean isAlreadyLoged = false;
		if (isLogMap.get(truncatedTime) == null) {

			boolean isLog = isLog();
			String hour = new DateTime(truncatedTime).toString("yyyy-MM-dd-HH");

			isLogMap.put(truncatedTime, isLog);
			log.info(
					"log kafka consumer, dataSource is %s, time is %s, is log %s",
					dataSource, hour, isLog);
			if (isLog) {
				BufferedWriter bw = null;
				String filePath = null;
				try {

					String fileName = String.format("%s_%s", dataSource, hour);
					filePath = LOG_DIR + "/" + fileName;

					boolean isFindOne = false;
					for (int i = 0; i < 20; ++i) {
						if (!new File(filePath).exists()) {
							isFindOne = true;
							break;
						}
						fileName += "_1";
						filePath = LOG_DIR + "/" + fileName;
					}

					if (isFindOne) {
						bw = new BufferedWriter(new FileWriter(filePath, true));
						log.info(
								"log kafka consumer, dataSource is %s, time is %s, log path is %s",
								dataSource, hour, filePath);
					} else {
						log.info(
								"log kafka consumer, can't create file after try, dataSource is %s, time is %s, log path is %s",
								dataSource, hour, filePath);
					}
				} catch (Exception e) {
					log.error(
							e,
							"log kafka consumer, failed to open file %s to log kafka consume",
							filePath);
				}
				logWriterMap.put(truncatedTime, bw);
			} else {
				logWriterMap.put(truncatedTime, null);
			}

			if (isLog && logWriterMap.get(truncatedTime) != null) {
				try {
					String msg = String.format(
							"curr:%s|partition:%s|count:%s|msg:%s\n",
							new DateTime(), partition, currentCount,
							row.getEvent());
					logWriterMap.get(truncatedTime).write(msg);
				} catch (Exception e) {
					log.error(
							e,
							"log kafka consumer, failed to append file writer %s to log kafka consume in create new file writer",
							logWriterMap.get(truncatedTime));
				}
				isAlreadyLoged = true;
			}

			java.util.Iterator<Entry<Long, BufferedWriter>> itr = logWriterMap
					.entrySet().iterator();
			while (itr.hasNext()) {
				Entry<Long, BufferedWriter> entry = itr.next();
				long currentTime = System.currentTimeMillis();
				if (entry.getKey() <= currentTime - (1800 + 3600) * 1000) {
					if (isLogMap.get(entry.getKey()) != null
							&& isLogMap.get(entry.getKey())
							&& logWriterMap.get(entry.getKey()) != null) {
						try {
							try {
								logWriterMap.get(entry.getKey()).write(
										"closed\n");
							} catch (Exception e) {
								log.error(
										e,
										"log kafka consumer, failed to close file writer %s.",
										logWriterMap.get(entry.getKey()));
							}
							logWriterMap.get(entry.getKey()).close();
						} catch (Exception e) {
							log.error(
									e,
									"log kafka consumer, failed to close file writer %s to log kafka consume",
									logWriterMap.get(entry.getKey()));
						}
					}
					isLogMap.remove(entry.getKey());
					itr.remove();
				}
			}
		}

		if (isLogMap.get(truncatedTime) != null && isLogMap.get(truncatedTime)
				&& !isAlreadyLoged && logWriterMap.get(truncatedTime) != null) {
			try {
				String msg = String
						.format("curr:%s|partition:%s|count:%s|msg:%s\n",
								new DateTime(), partition, currentCount,
								row.getEvent());
				logWriterMap.get(truncatedTime).write(msg);
			} catch (Exception e) {
				log.error(
						e,
						"log kafka consumer, failed to append file writer %s to log kafka consume",
						logWriterMap.get(truncatedTime));
			}
		}
	}

	public static void closeAll() {
		for (RealtimePlumberLogger logger : loggers.values()) {
			for (BufferedWriter writer : logger.logWriterMap.values()) {
				if (writer != null) {
					try {
						writer.close();
					} catch (Exception e) {
						log.error("close file writer error");
					}
				}
			}
		}
	}
}
