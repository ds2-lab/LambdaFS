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
package io.hops.metadata.yarn;

public class TablesDef {
      
  public static interface ApplicationAttemptStateTableDef {
    public static final String TABLE_NAME = "yarn_applicationattemptstate";
    public static final String APPLICATIONID = "applicationid";
    public static final String APPLICATIONATTEMPTID = "applicationattemptid";
    public static final String APPLICATIONATTEMPTSTATE =
        "applicationattemptstate";
    public static final String TRAKINGURL = "applicationattempttrakingurl";
  }
  
  public static interface ApplicationStateTableDef {
    public static final String TABLE_NAME = "yarn_applicationstate";
    public static final String APPLICATIONID = "applicationid";
    public static final String APPSTATE = "appstate";
    public static final String USER = "appuser";
    public static final String NAME = "appname";
    public static final String SMSTATE = "appsmstate";
  }
  
    public static interface DelegationKeyTableDef {
    public static final String TABLE_NAME = "yarn_delegation_key";
    public static final String KEY = "key";
    public static final String DELEGATIONKEY = "delegationkey";
  }

  public static interface DelegationTokenTableDef {
    public static final String TABLE_NAME = "yarn_delegation_token";
    public static final String SEQ_NUMBER = "seq_number";
    public static final String RMDT_IDENTIFIER = "rmdt_identifier";
  }

  public static interface ProjectQuotaTableDef {

    public static final String TABLE_NAME = "yarn_projects_quota";
    public static final String PROJECT_NAME = "projectname";
    public static final String REMAINING_QUOTA = "quota_remaining";
    public static final String TOTAL_USED_QUOTA = "total";
  }

  public static interface ProjectsDailyCostTableDef {

    public static final String TABLE_NAME = "yarn_projects_daily_cost";
    public static final String PROJECTNAME = "projectname";
    public static final String USER = "user";
    public static final String DAY = "day";
    public static final String CREDITS_USED = "credits_used";
    public static final String APP_IDS = "app_ids";
  }

  public static interface PriceMultiplicatorTableDef {

    public static final String TABLE_NAME = "yarn_price_multiplicator";
    public static final String ID = "id";
    public static final String MULTIPLICATOR = "multiplicator";
  }
  
  public static interface ReservationStateTableDef {

    public static final String TABLE_NAME = "yarn_reservation_state";
    public static final String PLANNAME = "plan_name";
    public static final String RESERVATIONIDNAME = "reservation_id_name";
    public static final String RESERVATIONSTATE = "state";
  }
  
   public static interface ConfMutationTableDef {

    public static final String TABLE_NAME = "yarn_conf_mutation";
    public static final String INDEX = "index";
    public static final String MUTATION = "mutation";
  }
   
  public static interface ConfTableDef {

    public static final String TABLE_NAME = "yarn_conf";
    public static final String INDEX = "index";
    public static final String CONF = "conf";
  } 
   
  public static interface AppProvenanceTableDef {

    public static final String TABLE_NAME = "yarn_app_provenance_log";
    public static final String ID = "id";
    public static final String NAME = "name";
    public static final String STATE = "state";
    public static final String USER = "user";
    public static final String TIMESTAMP = "timestamp";
    public static final String SUBMIT_TIME = "submit_time";
    public static final String START_TIME = "start_time";
    public static final String FINISH_TIME = "finish_time";
  }
}
