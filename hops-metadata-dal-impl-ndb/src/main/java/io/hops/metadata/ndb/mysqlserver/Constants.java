/*
 * Hops Database abstraction layer for storing the hops metadata in MySQL Cluster
 * Copyright (C) 2015  hops.io
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
package io.hops.metadata.ndb.mysqlserver;

public class Constants {
  public static final String PROPERTY_MYSQL_DATA_SOURCE_CLASS_NAME =
      "io.hops.metadata.ndb.mysqlserver.data_source_class_name";
  public static final String PROPERTY_MYSQL_HOST =
      "io.hops.metadata.ndb.mysqlserver.host";
  public static final String PROPERTY_MYSQL_PORT =
      "io.hops.metadata.ndb.mysqlserver.port";
  public static final String PROPERTY_MYSQL_CONNECTION_POOL_SIZE =
      "io.hops.metadata.ndb.mysqlserver.connection_pool_size";
  public static final String PROPERTY_MYSQL_USERNAME =
      "io.hops.metadata.ndb.mysqlserver.username";
  public static final String PROPERTY_MYSQL_PASSWORD =
      "io.hops.metadata.ndb.mysqlserver.password";
}
