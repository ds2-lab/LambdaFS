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
package io.hops.metadata.hdfs.entity;

import java.util.Map;


/**
 * right now it holds quota info. later we can add more
 * information like access time ( if we want to remove locks from the parent
 * dirs )
 */
public class DirectoryWithQuotaFeature implements Comparable<DirectoryWithQuotaFeature> {
  private Long inodeId;
  private Long nsQuota; /// NameSpace quota
  private Long nsUsed;
  private Long ssQuota; /// disk space quota
  private Long ssUsed;
  private Map<QuotaUpdate.StorageType, Long> typeQuota;
  private Map<QuotaUpdate.StorageType, Long> typeUsed;

  public DirectoryWithQuotaFeature(Long inodeId, Long nsQuota, Long nsCount,
      Long ssQuota, Long ssUsed, Map<QuotaUpdate.StorageType, Long> typeQuota,
      Map<QuotaUpdate.StorageType, Long> typeUsed) {
    this.inodeId = inodeId;
    this.nsQuota = nsQuota;
    this.nsUsed = nsCount;
    this.ssQuota = ssQuota;
    this.ssUsed = ssUsed;
    this.typeQuota = typeQuota;
    this.typeUsed = typeUsed;
  }

  public Long getInodeId() {
    return inodeId;
  }

  public Long getNsQuota() {
    return nsQuota;
  }

  public Long getSSQuota() {
    return ssQuota;
  }

  public void setInodeId(Long inodeId) {
    this.inodeId = inodeId;
  }

  public void setNsQuota(Long nsQuota) {
    this.nsQuota = nsQuota;
  }

  public void setSSQuota(Long dsQuota) {
    this.ssQuota = dsQuota;
  }

  public Long getNsUsed() {
    return nsUsed;
  }

  public void setNsUsed(Long nsUsed) {
    this.nsUsed = nsUsed;
  }

  public Long getSSUsed() {
    return ssUsed;
  }

  public void setSSUsed(Long dsUsed) {
    this.ssUsed = dsUsed;
  }

  public Map<QuotaUpdate.StorageType, Long> getTypeQuota() {
    return typeQuota;
  }

  public void setTypeQuota(Map<QuotaUpdate.StorageType, Long> typeQuota) {
    this.typeQuota = typeQuota;
  }

  public Map<QuotaUpdate.StorageType, Long> getTypeUsed() {
    return typeUsed;
  }

  public void setTypeUsed(Map<QuotaUpdate.StorageType, Long> typeUsed) {
    this.typeUsed = typeUsed;
  }
  
  public int compareTo(DirectoryWithQuotaFeature o) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String toString() {
    return "HopINodeAttributes{" +
        "inodeId=" + inodeId +
        ", nsQuota=" + nsQuota +
        ", nsCount=" + nsUsed +
        ", dsQuota=" + ssQuota +
        ", diskspace=" + ssUsed +
        '}';
  }
}
