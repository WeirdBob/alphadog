package com.weirdbob.alphadog;

import java.io.Closeable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.primitives.Longs;

public class AlphaDog implements Closeable {

	private CuratorFramework client;
	private Logger logger = LoggerFactory.getLogger(getClass());
	private boolean shallCloseClient;
	
	public AlphaDog(String zkConnectionString) {
        client = CuratorFrameworkFactory.newClient(zkConnectionString, new ExponentialBackoffRetry(1000, 3));
        client.start();
        shallCloseClient = true;
	}
	
	public AlphaDog(CuratorFramework client) {
        this.client = client;
        shallCloseClient = false;
	}
	
	public boolean runIfAlpha(Runnable r, String lockName) {
		Preconditions.checkArgument(!Strings.isNullOrEmpty(lockName), "lockName is not optionnal");
		String mutexPath = getLockPath(lockName);
		InterProcessSemaphoreMutex mutex = new InterProcessSemaphoreMutex(client, mutexPath);
        try {
			if (mutex.acquire(0, TimeUnit.MILLISECONDS)) {
				logger.debug("mutex acquired, saving timestamp");
				try {
					try {
						client.setData().forPath(mutexPath, Longs.toByteArray(LocalDateTime.now(ZoneId.systemDefault()).toEpochSecond(ZoneOffset.UTC)));
					} catch (Exception e) {
						logger.debug("Error writing last run time in parent node",e);
					}
					r.run();
					return true;
				} finally {
					mutex.release();
					logger.debug("mutex released");
				}
			} else {
				logger.debug("mutex NOT acquired");
			}
		} catch (Exception e) {
			logger.error("Error",e);
		}
        return false;
	}

	private String getLockPath(String lockName) {
		return "/alphadog-locks/"+lockName;
	}
	
	public boolean runIfAlpha(Runnable r, String lockName, Duration periodBetweenRuns) {
		Preconditions.checkArgument(!Strings.isNullOrEmpty(lockName), "lockName is not optionnal");
		String lockPath = getLockPath(lockName);
        try {
        	byte[] data = client.getData().forPath(lockPath);
        	if(data == null || data.length == 0) {
        		// never launched -> set last run to epoch
        		data = new byte[]{0,0,0,0,0,0,0,0};
        	}
    		LocalDateTime lastRun = LocalDateTime.ofEpochSecond(Longs.fromByteArray(data), 0, ZoneOffset.UTC);
    		logger.debug("Last run was "+lastRun);
    		LocalDateTime now = LocalDateTime.now(ZoneId.systemDefault());
    		Duration durationSinceLastRun = Duration.between(lastRun, now);
			if(periodBetweenRuns.compareTo(durationSinceLastRun) <= 0) {
    			return runIfAlpha(r, lockName);
    		} else {
        		logger.debug("Executions too close ("+durationSinceLastRun+" < "+periodBetweenRuns+")");
    		}
		} catch (Exception e) {
			logger.error("Error",e);
		}
        return false;
	}

	
	@Override
	public void close() {
		if(shallCloseClient) {
			logger.debug("closing client");
			client.close();
		}
	}
}
