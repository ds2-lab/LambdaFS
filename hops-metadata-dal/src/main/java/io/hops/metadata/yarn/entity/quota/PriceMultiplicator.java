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
package io.hops.metadata.yarn.entity.quota;

public class PriceMultiplicator {

  private final MultiplicatorType type;
  private final float price;

  public enum MultiplicatorType{
    GENERAL, GPU;
  }
  public PriceMultiplicator(MultiplicatorType type, float price) {
    this.type = type;
    this.price = price;
  }

  public MultiplicatorType getId() {
    return type;
  }


  public float getValue() {
    return price;
  }
  
  @Override
  public String toString() {
    return "YarnProjectsQuota{" + "type=" + type + ", price="
            + price + " }";
  }

}
