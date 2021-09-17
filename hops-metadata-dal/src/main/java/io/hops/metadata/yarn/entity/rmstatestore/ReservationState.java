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

package io.hops.metadata.yarn.entity.rmstatestore;

public class ReservationState {

  private final byte[] state;
  private final String planName;
  private final String reservationIdName;

  public ReservationState(byte[] state, String planName, String reservationIdName) {
    this.state = state;
    this.planName = planName;
    this.reservationIdName = reservationIdName;
  }

  public ReservationState(String planName, String reservationIdName) {
    this.state = null;
    this.planName = planName;
    this.reservationIdName = reservationIdName;
  }
  
  public byte[] getState() {
    return state;
  }

  public String getPlanName() {
    return planName;
  }

  public String getReservationIdName() {
    return reservationIdName;
  }
  
}
