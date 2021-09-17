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
package io.hops.log;

import org.apache.log4j.NDC;

public class NDCWrapper {

  private static boolean NDCEnabled = false;

  public static void enableNDC(boolean NDCEnamble) {
    NDCEnabled = NDCEnamble;
  }

  public static void push(String str) {
    if (NDCEnabled) {
      NDC.push(str);
    }
  }

  public static void pop() {
    if (NDCEnabled) {
      NDC.pop();
    }
  }

  public static void clear() {
    if (NDCEnabled) {
      NDC.clear();
    }
  }

  public static void remove() {
    if (NDCEnabled) {
      NDC.remove();
    }
  }

  public static String peek() {
    if (NDCEnabled) {
      return NDC.peek();
    } else {
      return "";
    }

  }

  public static boolean NDCEnabled(){
    return NDCEnabled;
  }
}
