package io.hops.metadata.ndb.wrapper;

import com.mysql.clusterj.core.util.Logger;
import org.apache.commons.logging.Log;

public class HopsClusterJLogger implements Logger {

  private static final String ENABLE_CLUSTERJ_LOGS_ENV = "ENABLE_CLUSTERJ_LOGS";
  private static boolean ENABLE_CLUSTERJ_LOGS = false;

  static {
    String isLogStr = System.getenv(ENABLE_CLUSTERJ_LOGS_ENV);
    try {
      if(isLogStr != null) {
        ENABLE_CLUSTERJ_LOGS = Boolean.parseBoolean(isLogStr);
      }
    } catch (Throwable t) {
      System.err.println("Unable to parse "+ENABLE_CLUSTERJ_LOGS_ENV+" env variable");
    }
  }

  private final Log LOG;

  HopsClusterJLogger(Log delegate) {
    LOG = delegate;
  }

  @Override
  public void detail(String s) {
    if (ENABLE_CLUSTERJ_LOGS)
      LOG.trace(s);
  }

  @Override
  public void debug(String s) {
    if (ENABLE_CLUSTERJ_LOGS)
      LOG.debug(s);
  }

  @Override
  public void trace(String s) {
    if (ENABLE_CLUSTERJ_LOGS)
      LOG.trace(s);
  }

  @Override
  public void info(String s) {
    if (ENABLE_CLUSTERJ_LOGS)
      LOG.info(s);
  }

  @Override
  public void warn(String s) {
    if (ENABLE_CLUSTERJ_LOGS)
      LOG.warn(s);
  }

  @Override
  public void error(String s) {
    if (ENABLE_CLUSTERJ_LOGS)
      LOG.error(s);
  }

  @Override
  public void fatal(String s) {
    if (ENABLE_CLUSTERJ_LOGS)
      LOG.fatal(s);
  }

  @Override
  public boolean isDetailEnabled() {
    return ENABLE_CLUSTERJ_LOGS && LOG.isTraceEnabled();
  }

  @Override
  public boolean isDebugEnabled() {
    return ENABLE_CLUSTERJ_LOGS && LOG.isDebugEnabled();
  }

  @Override
  public boolean isTraceEnabled() {
    return ENABLE_CLUSTERJ_LOGS && LOG.isTraceEnabled();
  }

  @Override
  public boolean isInfoEnabled() {
    return ENABLE_CLUSTERJ_LOGS && LOG.isInfoEnabled();
  }
}
