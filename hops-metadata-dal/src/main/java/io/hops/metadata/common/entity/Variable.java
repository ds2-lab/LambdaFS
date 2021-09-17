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
package io.hops.metadata.common.entity;

import io.hops.metadata.common.FinderType;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

public abstract class Variable {

  protected final static EnumMap<Finder, byte[]> defaultValues =
      new EnumMap(Finder.class);

  public static void registerVariableDefaultValue(Finder variable,
      byte[] defaultValue) {
    defaultValues.put(variable, defaultValue);
  }

  public static enum Finder implements FinderType<Variable> {

    //Named Variables
    BlockID(0),
    INodeID(1),
    QuotaUpdateID(2),
    ReplicationIndex(3),
    StorageInfo(4),
    BlockTokenKeys(5),
    BTCurrKey(6),
    BTNextKey(7),
    BTSimpleKey(8),
    SIdCounter(9),
    HdfsLeParams(10),
    YarnLeParams(11),
    MisReplicatedFilesIndex(12),
    SafeModeReached(13),
    BrLbMaxConcurrentBRs(14),
    RMStateStoreVersion(15),
    RMStateStoreEpoch(16),
    AMRMToken(17),
    RMDTSequenceNumber(18),
    QuotaTicksCounter(19),
    //Generic Variables
    GenericInteger(20),
    GenericLong(21),
    GenericString(22),
    GenericByteArray(23),
    GenericArray(24),
    StorageMap(26),  // TODO do we need this?
    CacheDirectiveID(27),
    neededScanCount(28),
    RollingUpgradeInfo(29),
    SafeModeInfo(30),
    GenericDouble(31),
    BlockTotal(32),
    BlockThreshold(33),
    BlockReplicationQueueThreshold(34),
    completedScanCount(35),
    curScanCount(36),
    FenceID(37),
    ConfigurationStoreVersion(38),
    RetryCacheCleanerEpoch(39),;

    private final int id;
    private static Map<Integer, Finder> idToFinder = new HashMap<>(Finder.values().length);

    private Finder(int id) {
        this.id = id;
    }

    public int getId() {
      return this.id;
    }

    public byte[] getDefaultValue() {
      return defaultValues.get(this);
    }

    public static Finder getFinder(int varType) {
      if (idToFinder.isEmpty()) {
        Finder[] Finders = Finder.values();
        for (Finder finder : Finders) {
          idToFinder.put(finder.getId(), finder);
        }
      }
      Finder finder = idToFinder.get(varType);
      if (finder == null) {
        throw new IllegalArgumentException(
            "Variable Type " + varType + " doesn't exist");
      }
      return finder;
    }

    @Override
    public Class getType() {
      return Variable.class;
    }

    @Override
    public Annotation getAnnotated() {
      return Annotation.PrimaryKey;
    }
  }

  private final Finder type;

  public Variable(Finder type) {
    this.type = type;
  }

  public abstract void setValue(byte[] val);

  public abstract byte[] getBytes();

  public abstract Object getValue();

  public abstract int getLength();

  public Finder getType() {
    return type;
  }

  public static Variable getVariable(byte varType) {
    Finder type = Finder.getFinder(varType);
    return getVariable(type);
  }

  public static Variable getVariable(Finder varType) {
    switch (varType) {

      case GenericInteger:
        return new IntVariable(varType);
      case GenericLong:
        return new LongVariable(varType);
      case GenericString:
        return new StringVariable(varType);
      case GenericByteArray:
        return new ByteArrayVariable(varType);
      case GenericArray:
        return new ArrayVariable(varType);
      case BlockID:
        return new LongVariable(varType);
      case INodeID:
        return new LongVariable(varType);
      case QuotaUpdateID:
        return new IntVariable(varType);
      case ReplicationIndex:
        return new ArrayVariable(varType);
      case StorageInfo:
        return new ArrayVariable(varType);
      case BlockTokenKeys:
        return new ArrayVariable(varType);
      case BTCurrKey:
      case BTNextKey:
      case BTSimpleKey:
        return new ByteArrayVariable(varType);
      case SIdCounter:
        return new IntVariable(varType);
      case HdfsLeParams:
      case YarnLeParams:
        return new StringVariable(varType);
      case MisReplicatedFilesIndex:
        return new LongVariable(varType);
      case SafeModeReached:
        return new LongVariable(varType);
      case BrLbMaxConcurrentBRs:
        return new LongVariable(varType);
      case RMStateStoreVersion:
        return new ByteArrayVariable(varType);
      case RMStateStoreEpoch:
        return new LongVariable(varType);
      case ConfigurationStoreVersion:
        return new ByteArrayVariable(varType);
      case AMRMToken:
        return new ByteArrayVariable(varType);
      case RMDTSequenceNumber:
        return new IntVariable(varType);
      case QuotaTicksCounter:
        return new LongVariable(varType);
      case CacheDirectiveID:
        return new LongVariable(varType);
      case neededScanCount:
        return new IntVariable(varType);
      case completedScanCount:
        return new IntVariable(varType);
      case curScanCount:
        return new IntVariable(varType);
      case RollingUpgradeInfo:
        return new ByteArrayVariable(varType);
      case SafeModeInfo:
        return new ArrayVariable(varType);
      case GenericDouble:
        return new DoubleVariable(varType);
      case BlockTotal:
      case BlockThreshold:
      case BlockReplicationQueueThreshold:
        return new IntVariable(varType);
      case FenceID:
        return new LongVariable(varType);
      case RetryCacheCleanerEpoch:
        return new LongVariable(varType);
    }
    return null;
  }

  public static Variable initVariable(Finder varType, byte[] varData) {
    Variable var = getVariable(varType);
    if (var != null) {
      var.setValue(varData);
    }
    return var;
  }
}
