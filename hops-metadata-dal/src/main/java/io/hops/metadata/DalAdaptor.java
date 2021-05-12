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
package io.hops.metadata;

import io.hops.exception.StorageException;

import java.util.ArrayList;
import java.util.Collection;

public abstract class DalAdaptor<HDFSClass, DALClass> {

  public Collection<DALClass> convertHDFStoDAL(
      Collection<HDFSClass> hdfsCollection) throws StorageException {
    Collection<DALClass> dalCollection = new ArrayList<>();
    if (hdfsCollection != null) {
      for (HDFSClass hdfsClass : hdfsCollection) {
        dalCollection.add(convertHDFStoDAL(hdfsClass));
      }
    }
    return dalCollection;
  }

  public abstract DALClass convertHDFStoDAL(HDFSClass hdfsClass)
      throws StorageException;

  public Collection<HDFSClass> convertDALtoHDFS(
      Collection<DALClass> dalCollection) throws StorageException {
    Collection<HDFSClass> hdfsCollection = null;
    if (dalCollection != null) {
      try {
        hdfsCollection = dalCollection.getClass().newInstance();
      } catch (InstantiationException | IllegalAccessException ex) {
        throw new StorageException(ex);
      }
      for (DALClass dalClass : dalCollection) {
        hdfsCollection.add(convertDALtoHDFS(dalClass));
      }
    }
    return hdfsCollection;

  }

  public abstract HDFSClass convertDALtoHDFS(DALClass dalClass)
      throws StorageException;
}
