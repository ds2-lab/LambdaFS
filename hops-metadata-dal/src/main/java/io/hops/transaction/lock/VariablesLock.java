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
package io.hops.transaction.lock;

import io.hops.metadata.common.entity.Variable;

import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;

public final class VariablesLock extends Lock {

  private final Map<Variable.Finder, TransactionLockTypes.LockType> variables;

  VariablesLock() {
    this.variables =
        new EnumMap<>(
            Variable.Finder.class);
  }

  VariablesLock addVariable(Variable.Finder variableType,
      TransactionLockTypes.LockType lockType) {
    this.variables.put(variableType, lockType);
    return this;
  }

  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    for (Map.Entry<Variable.Finder, TransactionLockTypes.LockType> e : variables
        .entrySet()) {
      acquireLock(e.getValue(), e.getKey());
    }
  }

  public TransactionLockTypes.LockType getVariableLockType(
      Variable.Finder variableType) {
    return variables.get(variableType);
  }

  @Override
  protected final Type getType() {
    return Type.Variable;
  }
}
