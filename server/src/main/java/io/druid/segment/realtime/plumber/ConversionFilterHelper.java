package io.druid.segment.realtime.plumber;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import com.metamx.emitter.EmittingLogger;

public class ConversionFilterHelper {
	private static final EmittingLogger log = new EmittingLogger(
			ConversionFilterHelper.class);
	private static final String CONVERSION_FILTER_CFG = "/dianyi/druid_config/conversion_filter_cfg.properties";
	private static JedisPool jedisPool = null;

	private static boolean enable = false;
	private static String host = "";
	private static int port = 6379;
	private static int keepTimeInSeconds = 172800;
	private static int windowTimeInSeconds = 3600;
	private static String prefix = "";
  
	private static String duplicateDealClass = "io.druid.segment.realtime.plumber.YeahMobiDuplicateRaw";
  
	private static volatile long lastLoadTime = 0L;
	
	private static boolean isFileExist() {
		File f = new File(CONVERSION_FILTER_CFG);
		if (f.exists() && f.isFile()) {
			return true;
		} else {
			return false;
		}
	}

	private static Properties getProperties() {
		Properties properties = new Properties();
		if (isFileExist()) {
			try {
				properties.load(new FileInputStream(CONVERSION_FILTER_CFG));
			} catch (Exception e) {
				log.error(e, "read conversion filter log error "
						+ CONVERSION_FILTER_CFG);
			}
		}
		return properties;
	}

	private static void loadCfg() {
		
		if(System.currentTimeMillis() - lastLoadTime < (3600L * 1000L)){
			return;
		}

		try {
			if (isFileExist()) {

				Properties p = getProperties();

				host = p.getProperty("redis.host").toString().trim();
				port = Integer.parseInt(getProperties().get("redis.port")
						.toString().trim());

				keepTimeInSeconds = Integer.parseInt(p.get(
						"conversion.keep.time").toString().trim());
				windowTimeInSeconds = Integer.parseInt(p.get(
						"conversion.window.time").toString().trim());
				prefix = p.getProperty("conversion.set.prefix").toString().trim();
				if(null != p.getProperty("duplicate.deal.class")){					
					duplicateDealClass = p.getProperty("duplicate.deal.class");
				}

				enable = true;

			} else {
				enable = false;
			}
		} catch (Exception e) {
			log.error(e, "read conversion filter log error "
					+ CONVERSION_FILTER_CFG);
		}finally{
			lastLoadTime = System.currentTimeMillis();
		}
	}

	public static boolean isConversionFilterEnable() {
		loadCfg();
		return enable;
	}

	public synchronized static JedisPool getPool() {
		loadCfg();
		if (null == jedisPool) {
			JedisPoolConfig config = new JedisPoolConfig();
			config.setMaxActive(250);
			config.setMaxIdle(200);
			config.setMaxWait(1000);

			jedisPool = new JedisPool(config, host, port, 10000);
		}

		return jedisPool;
	}

	/**
	 * 
	 * @return seconds
	 */
	public static int getKeepTime() {
		loadCfg();
		return keepTimeInSeconds;
	}

	/**
	 * 
	 * @return seconds
	 */
	public static int getWindowsTime() {
		loadCfg();
		return windowTimeInSeconds;
	}

	public static String getPrefix() {
		loadCfg();
		return prefix;
	}
	public static String getDuplicateDealClass(){
		return duplicateDealClass;
	}
}
