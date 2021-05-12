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
package io.hops.metadata.election.entity;

import io.hops.metadata.common.FinderType;
import io.hops.metadata.common.entity.Variable;

public abstract class LeDescriptorFactory {
  
  public abstract Variable.Finder getVarsFinder();
  
  public abstract FinderType<LeDescriptor> getAllFinder();
  
  public abstract FinderType<LeDescriptor> getByIdFinder();
  
  public abstract LeDescriptor cloneDescriptor(LeDescriptor desc);
  
  public abstract LeDescriptor getNewDescriptor(long id, long counter,
      String hostName, String httpAddress, byte locationDomainId);
  
}
