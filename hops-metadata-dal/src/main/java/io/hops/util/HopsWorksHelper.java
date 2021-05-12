/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package io.hops.util;

public class HopsWorksHelper {

  public static String getUserName(String totalName){
    String[] components = totalName.split("__");
    if(components.length>=2){
      return components[1];
    }else{
      return totalName;
    }
  }
  
  public static String getProjectName(String totalName){
    return totalName.split("__")[0];
  }
}
