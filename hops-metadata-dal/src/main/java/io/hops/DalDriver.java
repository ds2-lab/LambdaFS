/*
 * Copyright (C) 2015 hops.io.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops;

import io.hops.events.EventManager;
import io.hops.exception.StorageInitializtionException;

public class DalDriver {

  /**
   * Load an instance of EventManager specified by the given fully-qualified class name.
   */
  public static EventManager loadEventManager(String eventManagerClassName)
          throws StorageInitializtionException{
    try {
      Object instance = Class.forName(eventManagerClassName).newInstance();
      return (EventManager)instance;
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException ex) {
      throw new StorageInitializtionException(ex);
    }
  }

  public static DalStorageFactory load(String storageFactoryClassName)
      throws StorageInitializtionException {
    try {
      System.out.println("Attempting to load storage factory class with class name: \"" + storageFactoryClassName + "\"");

      /*ClassLoader loader = DalStorageFactory.class.getClassLoader();
      System.out.println(DalStorageFactory.class.getSimpleName() + ".class");
      System.out.println(String.valueOf(DalStorageFactory.class.getResource("DalStorageFactory.class")));
      System.out.println(String.valueOf(loader.getResource("io/hops/DalStorageFactory.class")));
      System.out.println(String.valueOf(DalStorageFactory.class.getProtectionDomain().getCodeSource().getLocation()));*/

      System.out.println("Attempting to create instance of class " + storageFactoryClassName + " without casting it to DalStorageFactory...");

      Object instance = Class.forName(storageFactoryClassName).newInstance();

      System.out.println("instance.getClass() = " + instance.getClass().toString());
      System.out.println(instance);

      return (DalStorageFactory)instance;

      // return (DalStorageFactory) Class.forName(storageFactoryClassName).newInstance();
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException ex) {
      throw new StorageInitializtionException(ex);
    }
  }
  public static DalEventStreaming loadHopsEventStreamingLib(
          String storageFactoryClassName) throws StorageInitializtionException {
    try {
      return (DalEventStreaming) Class.forName(storageFactoryClassName).newInstance();
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException ex) {
      throw new StorageInitializtionException(ex);
    }
  }
}
