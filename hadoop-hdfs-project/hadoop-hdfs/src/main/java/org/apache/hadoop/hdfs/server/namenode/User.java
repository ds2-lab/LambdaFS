package org.apache.hadoop.hdfs.server.namenode;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;

@PersistenceCapable(table = "user", database = "world")
public interface User {

    @PrimaryKey
    int getId();

    void setId(int id);

    @Column(name = "first")
    String getFirstName();

    void setFirstName(String firstName);

    @Column(name = "last")
    String getLastName();

    void setLastName(String lastName);

    @Column(name = "position")
    String getPosition();

    void setPosition(String position);

    @Column(name = "department")
    String getDepartment();

    void setDepartment(String department);
}