package azkaban.webapp;

import azkaban.spi.SchedException;
import azkaban.spi.SchedulerInterface;
import azkaban.spi.ScheduleMetadata;
import org.apache.log4j.Logger;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.Scheduler;


/**
 * For simple implementation now, we don't consider Quartz misfire.
 */
public class QuartzScheduler implements SchedulerInterface {

  private static final Logger logger = Logger.getLogger(AzkabanWebServer.class);

  private Scheduler scheduler;

  @Override
  public void init() {
    try {
      scheduler = new StdSchedulerFactory("config/scout-quartz.properties")
          .getScheduler();
      scheduler.start();
      logger.info("Quartz Scheduler initialized");
    } catch (SchedulerException e) {
      logger.error("An exception occured when initializing Quartz Interface ", e);
      throw new SchedException(e);
    }

  }

  @Override
  public void shutdown() {
    try {
      // Waiting for jobs to finish
      logger.info("QuartzScheduler shutting down, waiting for all tasks to end...");
      scheduler.shutdown(true);
      logger.info("QuartzScheduler completely shut down");
    } catch (SchedulerException e) {
      logger.error("Failed to shut down QuartzScheduler, an exception occured ", e);
      throw new SchedException(e);
    }
  }

  @Override
  public void suspend() {
    try {
      scheduler.standby();
    } catch (SchedulerException e) {
      logger.error("Failed to stand by QuartzScheduler, an exception occured ", e);
      throw new SchedException(e);
    }
  }

  @Override
  public void resume() {
    try {
      scheduler.start();
    } catch (SchedulerException e) {
      logger.error("An exception occured when initializing Quartz Interface ", e);
      throw new SchedException(e);
    }
  }

  @Override
  public void addJob(ScheduleMetadata job) {

  }

  @Override
  public void delete(int jobId) {

  }

  @Override
  public void update(ScheduleMetadata job) {

  }
}
