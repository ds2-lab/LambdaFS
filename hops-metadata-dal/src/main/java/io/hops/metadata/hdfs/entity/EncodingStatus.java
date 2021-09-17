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

import io.hops.metadata.common.CounterType;
import io.hops.metadata.common.FinderType;

/**
 * This class represents the state of an erasure-coded file and its parity file.
 * This includes the health as well as on-going encoding or repairs.
 */
public class EncodingStatus {

  /**
   * The erasure coding state of the file.
   */
  public static enum Status {
    /** This is not an erasure-coded file */
    NOT_ENCODED,
    /** The encoding was requested */
    ENCODING_REQUESTED,
    /** Encoding was requested and the file need to be rewritten to ensure
     * placement constraints */
    COPY_ENCODING_REQUESTED,
    /** The encoding is active */
    ENCODING_ACTIVE,
    /** The encoding was canceled */
    ENCODING_CANCELED,
    /** The encoding failed */
    ENCODING_FAILED,
    /** The file is encoded and healthy */
    ENCODED,
    /** The file is corrupt and a repair was requested  */
    REPAIR_REQUESTED,
    /** A repair is active */
    REPAIR_ACTIVE,
    /** A repair was canceled */
    REPAIR_CANCELED,
    /** A repair attempt failed */
    REPAIR_FAILED,
    /** The file was deleted. Intermediate state before being cleaned. */
    DELETED,
  }

  /**
   * The state of the parity file.
   */
  public static enum ParityStatus {
    /** All blocks are available and not corrupted */
    HEALTHY,
    /** Corruption was detected and a repair was requested */
    REPAIR_REQUESTED,
    /** A repair is active */
    REPAIR_ACTIVE,
    /** A repair was canceled */
    REPAIR_CANCELED,
    /** A repair attempt failed */
    REPAIR_FAILED
  }

  /** Internal counter to collect different statistics. */
  public static enum Counter implements CounterType<EncodingStatus> {
    /** Count all requested encodings */
    RequestedEncodings,
    /** Count all active encodings */
    ActiveEncodings,
    /** Count all active repairs */
    ActiveRepairs,
    /** Count all encoded files */
    Encoded;

    @Override
    public Class getType() {
      return EncodingStatus.class;
    }
  }

  /**
   * Internal finder to retrieve the status from the database.
   */
  public static enum Finder implements FinderType<EncodingStatus> {
    /** Use the file id to find the status */
    ByInodeId,
    ByInodeIds,
    /** Use the parity file id to find the status */
    ByParityInodeId,
    ByParityInodeIds;

    @Override
    public Class getType() {
      return EncodingStatus.class;
    }

    @Override
    public Annotation getAnnotated() {
      switch (this) {
        case ByInodeId:
          return Annotation.PrimaryKey;
        case ByInodeIds:
          return Annotation.Batched;
        case ByParityInodeId:
          return Annotation.IndexScan;
        case ByParityInodeIds:
          return Annotation.IndexScan;
        default:
          throw new IllegalStateException();
      }
    }

  }

  private Long inodeId;
  private boolean inTree;
  private Long parityInodeId;
  private Status status;
  private ParityStatus parityStatus;
  private EncodingPolicy encodingPolicy;
  private Long statusModificationTime;
  private Long parityStatusModificationTime;
  private String parityFileName;
  private Integer lostBlocks;
  private Integer lostParityBlocks;
  private Boolean revoked;

  /**
   * Construct an EncodingStatus.
   */
  public EncodingStatus() {

  }

  /**
   * Construct an EncodingStatus.
   *
   * @param status
   *    the status of the file
   */
  public EncodingStatus(Status status) {
    this.status = status;
  }

  /**
   * Construct an Encoding status.
   *
   * @param status
   *    the status of the file
   * @param encodingPolicy
   *    the encoding policy of the file
   * @param parityFileName
   *    the name of the parity file
   */
  public EncodingStatus(Status status, EncodingPolicy encodingPolicy,
      String parityFileName) {
    this.status = status;
    this.encodingPolicy = encodingPolicy;
    this.parityFileName = parityFileName;
  }

  /**
   * Construct an EncodingStatus.
   *
   * @param inodeId
   *    the inode id of the file
   * @param status
   *    the status of the file
   * @param encodingPolicy
   *    the encoding policy of the file
   * @param statusModificationTime
   */
  public EncodingStatus(Long inodeId, Status status,
      EncodingPolicy encodingPolicy, Long statusModificationTime) {
    this(inodeId, true, status, encodingPolicy, statusModificationTime);
  }
  
  public EncodingStatus(Long inodeId, boolean inTree, Status status,
      EncodingPolicy encodingPolicy, Long statusModificationTime) {
    this.inodeId = inodeId;
    this.inTree = inTree;
    this.status = status;
    this.encodingPolicy = encodingPolicy;
    this.statusModificationTime = statusModificationTime;
  }

  /**
   * Construct an EncodingStatus.
   *
   * @param inodeId
   *    the inode id of the file
   * @param parityInodeId
   *    the inode id of the parity file
   * @param status
   *    the status of the file
   * @param parityStatus
   *    the status of the parity file
   * @param encodingPolicy
   *    the encoding policy of the file
   * @param statusModificationTime
   *    the timestamp representing the latest change of the status
   * @param parityStatusModificationTime
   *    the timestamp representing the latest change of the parity status
   * @param parityFileName
   *    the name of the parity file
   * @param lostBlocks
   *    the number of corrupted blocks
   * @param lostParityBlocks
   *    the number of corrupted parity blockes
   * @param revoked
   *    true if the encoding was revoked
   */
  public EncodingStatus(Long inodeId, Long parityInodeId, Status status,
      ParityStatus parityStatus, EncodingPolicy encodingPolicy,
      Long statusModificationTime, Long parityStatusModificationTime,
      String parityFileName, Integer lostBlocks, Integer lostParityBlocks,
      Boolean revoked) {
    this(inodeId, true, parityInodeId, status, parityStatus, encodingPolicy, statusModificationTime,
        parityStatusModificationTime, parityFileName, lostBlocks, lostParityBlocks, revoked);
  }
  
  public EncodingStatus(Long inodeId, boolean inTree, Long parityInodeId, Status status,
      ParityStatus parityStatus, EncodingPolicy encodingPolicy,
      Long statusModificationTime, Long parityStatusModificationTime,
      String parityFileName, Integer lostBlocks, Integer lostParityBlocks,
      Boolean revoked) {
    this.inodeId = inodeId;
    this.inTree = inTree;
    this.parityInodeId = parityInodeId;
    this.status = status;
    this.parityStatus = parityStatus;
    this.encodingPolicy = encodingPolicy;
    this.statusModificationTime = statusModificationTime;
    this.parityStatusModificationTime = parityStatusModificationTime;
    this.parityFileName = parityFileName;
    this.lostBlocks = lostBlocks;
    this.lostParityBlocks = lostParityBlocks;
    this.revoked = revoked;
  }

  public EncodingStatus(EncodingStatus other) {
    this.inodeId = other.inodeId;
    this.inTree = other.inTree;
    this.parityInodeId = other.parityInodeId;
    this.status = other.status;
    this.parityStatus = other.parityStatus;
    this.encodingPolicy = other.encodingPolicy;
    this.statusModificationTime = other.statusModificationTime;
    this.parityStatusModificationTime = other.parityStatusModificationTime;
    this.parityFileName = other.parityFileName;
    this.lostBlocks = other.lostBlocks;
    this.lostParityBlocks = other.lostParityBlocks;
    this.revoked = other.revoked;
  }

  /**
   * Get the inode id of the file.
   *
   * @return
   *    the inode id of the file
   */
  public Long getInodeId() {
    return inodeId;
  }

  public boolean isInTree(){
    return inTree;
  }
  
  /**
   * Set the inode id of the file
   *
   * @param inodeId
   *    the inode id of the file
   */
  public void setInodeId(Long inodeId) {
    setInodeId(inodeId, true);
  }
  
  public void setInodeId(Long inodeId, boolean inTree) {
    this.inodeId = inodeId;
    this.inTree = inTree;
  }

  /**
   * Get the inode id of the parity file
   *
   * @return
   *    the inode id of the parity file
   */
  public Long getParityInodeId() {
    return parityInodeId;
  }

  /**
   * Set the inode id of the parity file.
   *
   * @param parityInodeId
   *    the inode id of the parity ifle
   */
  public void setParityInodeId(Long parityInodeId) {
    this.parityInodeId = parityInodeId;
  }

  /**
   * Get the status of the file.
   *
   * @return
   *    the status of the file
   */
  public Status getStatus() {
    return status;
  }

  /**
   * Set the status of the file.
   *
   * @param status
   *    the status of the file
   */
  public void setStatus(Status status) {
    this.status = status;
  }

  /**
   * Get the status of the parity file.
   *
   * @return
   *    the status of the parity file
   */
  public ParityStatus getParityStatus() {
    return parityStatus;
  }

  /**
   * Set the status of the parity file.
   *
   * @param parityStatus
   *    the status of the parity file
   */
  public void setParityStatus(ParityStatus parityStatus) {
    this.parityStatus = parityStatus;
  }

  /**
   * Get the encoding policy.
   *
   * @return
   *  the encoding policy
   */
  public EncodingPolicy getEncodingPolicy() {
    return encodingPolicy;
  }

  /**
   * Set the encoding policy.
   *
   * @param encodingPolicy
   *    the encoding policy
   */
  public void setEncodingPolicy(EncodingPolicy encodingPolicy) {
    this.encodingPolicy = encodingPolicy;
  }

  /**
   * Get the timestamp representing the time when the status was modified.
   *
   * @return
   *    a unix timestamp representing the time when the status was modified
   */
  public Long getStatusModificationTime() {
    return statusModificationTime;
  }

  /**
   * Set the timestamp representing the time when the status was modified.
   *
   * @param statusModificationTime
   *    a unix timestamp representing the time when the status was modified
   */
  public void setStatusModificationTime(Long statusModificationTime) {
    this.statusModificationTime = statusModificationTime;
  }

  /**
   * Get the timestamp representing the time when the parity status was modified.
   *
   * @return
   *    a unix timestamp representing the time when the parity status was modified
   */
  public Long getParityStatusModificationTime() {
    return parityStatusModificationTime;
  }

  /**
   * Set the timestamp representing the time when the parity status was modified.
   *
   * @param parityStatusModificationTime
   *    a unix timestamp representing the time when the parity status was modified
   */
  public void setParityStatusModificationTime(
      Long parityStatusModificationTime) {
    this.parityStatusModificationTime = parityStatusModificationTime;
  }

  /**
   * Get the name of the parity file.
   *
   * @return
   *    the name of the parity file
   */
  public String getParityFileName() {
    return parityFileName;
  }

  /**
   * Set the name of the parity file.
   *
   * @param parityFileName
   *    the name of the parity file
   */
  public void setParityFileName(String parityFileName) {
    this.parityFileName = parityFileName;
  }

  /**
   * Get the number of corrupted blocks.
   *
   * @return
   *    the number of corrupted blocks.
   */
  public Integer getLostBlocks() {
    return lostBlocks;
  }

  /**
   * Set the number of corrupted blocks.
   *
   * @param lostBlocks
   *    the number of corrupted blocks.
   */
  public void setLostBlocks(Integer lostBlocks) {
    this.lostBlocks = lostBlocks;
  }

  /**
   * Get the number of corrupted parity blocks.
   *
   * @return
   *    the number of corrupted parity blocks.
   */
  public Integer getLostParityBlocks() {
    return lostParityBlocks;
  }

  /**
   * Set the number of corrupted parity blocks.
   *
   * @param lostParityBlocks
   *    the number of corrupted parity blocks.
   */
  public void setLostParityBlocks(Integer lostParityBlocks) {
    this.lostParityBlocks = lostParityBlocks;
  }

  /**
   * Get the revocation status of the file.
   *
   * @return
   *    true if undoing of the encoding was requested
   */
  public Boolean getRevoked() {
    return revoked;
  }

  /**
   * Set the revocation status of the file.
   *
   * @param revoked
   *    revocation status of the file
   */
  public void setRevoked(Boolean revoked) {
    this.revoked = revoked;
  }

  /**
   * Return if the file is encoded or encoding was requested.
   *
   * @return
   *    true if the file is encoded or encoding was requested
   */
  public boolean isEncoded() {
    switch (status) {
      case ENCODED:
      case REPAIR_REQUESTED:
      case REPAIR_ACTIVE:
      case REPAIR_CANCELED:
      case REPAIR_FAILED:
        return true;
      default:
        return false;
    }
  }

  /**
   * Return whether the file is corrupt or not. This does not consider the state
   * of the parity file.
   *
   * @return
   *    true if the file has corrupted blocks
   */
  public boolean isCorrupt() {
    switch (status) {
      case REPAIR_REQUESTED:
      case REPAIR_ACTIVE:
      case REPAIR_CANCELED:
      case REPAIR_FAILED:
        return true;
      default:
        return false;
    }
  }

  /**
   * Return whether a parity file repair is active.
   *
   * @return
   *    true if a parity file repair is active
   */
  public boolean isParityRepairActive() {
    switch (parityStatus) {
      case REPAIR_ACTIVE:
        return true;
      default:
        return false;
    }
  }

  /**
   * Return whether the parity file is corrupt or not.
   *
   * @return
   *    true if the parity file is corrupt
   */
  public boolean isParityCorrupt() {
    switch (parityStatus) {
      case REPAIR_ACTIVE:
      case REPAIR_REQUESTED:
      case REPAIR_CANCELED:
      case REPAIR_FAILED:
        return true;
      default:
        return false;
    }
  }

  @Override
  public String toString() {
    return "EncodingStatus{" +
        "inodeId=" + inodeId +
        ", parityInodeId=" + parityInodeId +
        ", status=" + status +
        ", parityStatus=" + parityStatus +
        ", encodingPolicy=" + encodingPolicy +
        ", statusModificationTime=" + statusModificationTime +
        ", parityStatusModificationTime=" + parityStatusModificationTime +
        ", parityFileName='" + parityFileName + '\'' +
        ", lostBlocks=" + lostBlocks +
        ", lostParityBlocks=" + lostParityBlocks +
        ", revoked=" + revoked +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    EncodingStatus status1 = (EncodingStatus) o;

    if (encodingPolicy != null ?
        !encodingPolicy.equals(status1.encodingPolicy) :
        status1.encodingPolicy != null) {
      return false;
    }
    if (inodeId != null ? !inodeId.equals(status1.inodeId) :
        status1.inodeId != null) {
      return false;
    }
    if (lostBlocks != null ? !lostBlocks.equals(status1.lostBlocks) :
        status1.lostBlocks != null) {
      return false;
    }
    if (lostParityBlocks != null ?
        !lostParityBlocks.equals(status1.lostParityBlocks) :
        status1.lostParityBlocks != null) {
      return false;
    }
    if (parityFileName != null ?
        !parityFileName.equals(status1.parityFileName) :
        status1.parityFileName != null) {
      return false;
    }
    if (parityInodeId != null ? !parityInodeId.equals(status1.parityInodeId) :
        status1.parityInodeId != null) {
      return false;
    }
    if (parityStatus != status1.parityStatus) {
      return false;
    }
    if (parityStatusModificationTime != null ? !parityStatusModificationTime
        .equals(status1.parityStatusModificationTime) :
        status1.parityStatusModificationTime != null) {
      return false;
    }
    if (revoked != null ? !revoked.equals(status1.revoked) :
        status1.revoked != null) {
      return false;
    }
    if (status != status1.status) {
      return false;
    }
    if (statusModificationTime != null ?
        !statusModificationTime.equals(status1.statusModificationTime) :
        status1.statusModificationTime != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = inodeId != null ? inodeId.hashCode() : 0;
    result =
        31 * result + (parityInodeId != null ? parityInodeId.hashCode() : 0);
    result = 31 * result + (status != null ? status.hashCode() : 0);
    result = 31 * result + (parityStatus != null ? parityStatus.hashCode() : 0);
    result =
        31 * result + (encodingPolicy != null ? encodingPolicy.hashCode() : 0);
    result = 31 * result +
        (statusModificationTime != null ? statusModificationTime.hashCode() :
            0);
    result = 31 * result + (parityStatusModificationTime != null ?
        parityStatusModificationTime.hashCode() : 0);
    result =
        31 * result + (parityFileName != null ? parityFileName.hashCode() : 0);
    result = 31 * result + (lostBlocks != null ? lostBlocks.hashCode() : 0);
    result = 31 * result +
        (lostParityBlocks != null ? lostParityBlocks.hashCode() : 0);
    result = 31 * result + (revoked != null ? revoked.hashCode() : 0);
    return result;
  }
}
