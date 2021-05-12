package io.hops.metadata.ndb.wrapper;
import com.mysql.clusterj.core.util.JDK14LoggerFactoryImpl;
import com.mysql.clusterj.core.util.Logger;
import com.mysql.clusterj.core.util.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HopsLoggerFactory implements LoggerFactory{

  static final Map<String, Logger> loggerMap = new HashMap<String, Logger>();

  public HopsLoggerFactory() {
    // create all the known loggers for the core project
    registerLogger(JDK14LoggerFactoryImpl.CLUSTERJ_LOGGER);
    registerLogger(JDK14LoggerFactoryImpl.CLUSTERJ_METADATA_LOGGER);
    registerLogger(JDK14LoggerFactoryImpl.CLUSTERJ_QUERY_LOGGER);
    registerLogger(JDK14LoggerFactoryImpl.CLUSTERJ_UTIL_LOGGER);
  }

  public Logger registerLogger(String loggerName) {
    final Log log = LogFactory.getLog(loggerName);
    Logger result = new HopsClusterJLogger(log);
    loggerMap.put(loggerName, result);
    return result;
  }

  @SuppressWarnings("unchecked")
  public Logger getInstance(Class cls) {
    String loggerName = getPackageName(cls);
    return getInstance(loggerName);
  }

  public synchronized Logger getInstance(String loggerName) {
    Logger result = loggerMap.get(loggerName);
    if (result == null) {
      result = registerLogger(loggerName);
    }
    return result;
  }

  /**
   * Returns the package portion of the specified class.
   * @param cls the class from which to extract the
   * package
   * @return package portion of the specified class
   */
  final private static String getPackageName(Class<?> cls)
  {
    String className = cls.getName();
    int index = className.lastIndexOf('.');
    return ((index != -1) ? className.substring(0, index) : ""); // NOI18N
  }
}
