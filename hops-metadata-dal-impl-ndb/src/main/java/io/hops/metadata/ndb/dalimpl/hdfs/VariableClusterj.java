/*
 * Hops Database abstraction layer for storing the hops metadata in MySQL Cluster
 * Copyright (C) 2015  hops.io
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package io.hops.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.common.entity.Variable;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.VariableDataAccess;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.log4j.Logger;

public class VariableClusterj
    implements TablesDef.VariableTableDef, VariableDataAccess<Variable, Variable.Finder> {
  static final Logger LOG = Logger.getLogger(VariableClusterj.class);

  @PersistenceCapable(table = TABLE_NAME)
  public interface VariableDTO {

    @PrimaryKey
    @Column(name = ID)
    int getId();

    void setId(int id);

    @Column(name = VARIABLE_VALUE)
    byte[] getValue();

    void setValue(byte[] value);
  }

  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public Variable getVariable(Variable.Finder varType) throws StorageException {
    //[S] Under read intensive load this may fail. Retry before throwing an
    // exception. The assumption is that the variable are added to the database
    // on cluster start / format. Getting null value is unexpected.
    VariableDTO vd = null;
    HopsSession session = connector.obtainSession();
    int retryCount = 0;
    do{
      vd = session.find(VariableDTO.class, varType.getId());
      if(vd == null){
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        LOG.warn("Unable to read variable id="+varType.getId()+". retry " +
                "count: " + retryCount);
      }
    }while(vd == null && retryCount++<10);

    if (vd == null) {
      throw new StorageException(
              "There is no variable entry with id " + varType.getId());
    }
    Variable var = Variable.initVariable(varType, vd.getValue());
    session.release(vd);
    return var;
  }

  @Override
  public void setVariable(Variable var) throws StorageException {
    HopsSession session = connector.obtainSession();
    VariableDTO vd = null;
    try {
      vd = createVariableDTO(session, var);
      session.savePersistent(vd);
    }finally {
      session.release(vd);
    }
  }

  @Override
  public void prepare(Collection<Variable> newVariables,
      Collection<Variable> updatedVariables,
      Collection<Variable> removedVariables) throws StorageException {
    HopsSession session = connector.obtainSession();
    removeVariables(session, removedVariables);
    updateVariables(session, newVariables);
    updateVariables(session, updatedVariables);
  }

  private void removeVariables(HopsSession session, Collection<Variable> vars)
      throws StorageException {
    if (vars != null) {
      List<VariableDTO> removed = new ArrayList<>();
      try {
        for (Variable var : vars) {
          VariableDTO vd =
              session.newInstance(VariableDTO.class, var.getType().getId());
          removed.add(vd);
        }
        session.deletePersistentAll(removed);
      }finally {
        session.release(removed);
      }
    }
  }

  private void updateVariables(HopsSession session, Collection<Variable> vars)
      throws StorageException {
    List<VariableDTO> changes = new ArrayList<>();
    try {
      for (Variable var : vars) {
        changes.add(createVariableDTO(session, var));
      }
      session.savePersistentAll(changes);
    }finally {
      session.release(changes);
    }
  }

  private VariableDTO createVariableDTO(HopsSession session, Variable var)
      throws StorageException {
    byte[] varVal = var.getBytes();
    if (varVal.length > MAX_VARIABLE_SIZE) {
      throw new StorageException("wrong variable size" + varVal.length +
          ", variable size should be less or equal to " + MAX_VARIABLE_SIZE);
    }
    VariableDTO vd = session.newInstance(VariableDTO.class);
    vd.setValue(var.getBytes());
    vd.setId(var.getType().getId());
    return vd;
  }
}
