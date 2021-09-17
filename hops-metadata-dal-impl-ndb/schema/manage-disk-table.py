#!/usr/bin/python

import os
import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-ls', action="store" ,  dest="logFileSize", default="1024M", help="Log File Size") 
parser.add_argument('-lc', action="store" ,  dest="logFilesCount", default=1, type=int, help="Log File Count") 
parser.add_argument('-ds', action="store" ,  dest="dataFileSize", default="1024M", help="Data File Size") 
parser.add_argument('-dc', action="store" ,  dest="dataFilesCount", default=1, type=int, help="Data File Count") 
parser.add_argument('-u', action="store" ,  dest="userName", default="", help="MySQL User Name") 
parser.add_argument('-p', action="store" ,  dest="passwd", default="", help="MySQL User Passwd") 
parser.add_argument('-H', action="store" ,  dest="host", default="", help="MySQL Host") 
parser.add_argument('-P', action="store" ,  dest="port", default="3306", help="MySQL Port") 
parser.add_argument('-db', action="store" ,  dest="database", default="", help="MySQL Database") 
group = parser.add_mutually_exclusive_group()
group.add_argument('-Create', action='store_true', dest="create", help="Create Files and Tables")
group.add_argument('-Drop', action='store_true', dest="drop", help="Drop Files and Tables");
args = parser.parse_args()
print args

#DATA_DISKS = ["/dir1", "/dir2"]
DATA_DISKS = [""]
DATAFILE_SIZE = args.dataFileSize 
DATAFILES_PER_DISK = args.dataFilesCount 

#LOG_DISKS = ["/dir1", "/dir2"]
LOG_DISKS = [""]
LOGFILE_SIZE = args.logFileSize
LOGFILES_PER_DISK = args.logFilesCount 

CONNECT_STRING = "MYSQL_PWD="+args.passwd+" mysql -u"+args.userName+" -P"+args.port+" -h "+args.host+" "+args.database+" -e "
print CONNECT_STRING


ONDISK_SMALL_FILE_INODE_SIZE=2000
ONDISK_MEDIUM_FILE_INODE_SIZE=4000
ONDISK_LARGE_FILE_INODE_SIZE=8000
TS_NAME="ts_1"

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def printStage(message):
   message=("\n*** %s ***\n"% (message))
   print(bcolors.OKBLUE+bcolors.BOLD+message+bcolors.ENDC)

def executeSQLCommand(subCmd):
  command = ('%s"%s"' % (CONNECT_STRING, subCmd))
  print("*** "+command)
  os.system(command)


def create():
  # create log files
  logGroupCreated = False
  for fileIndex in range(0, LOGFILES_PER_DISK):
    for disk in LOG_DISKS:
      if disk and not disk.endswith('/'):
        disk+="/" 

      if logGroupCreated == False:
        printStage("Creating Log Group")
        logGroupCreated = True
        subCommand = (
          "CREATE LOGFILE GROUP lg_1 ADD UNDOFILE '%sundo_log_%d.log' INITIAL_SIZE = %s ENGINE ndbcluster" % (
            disk, fileIndex, LOGFILE_SIZE))
      else:
        subCommand = (
          "ALTER LOGFILE GROUP lg_1 ADD UNDOFILE '%sundo_log_%d.log' INITIAL_SIZE = %s ENGINE ndbcluster" % (
            disk, fileIndex, LOGFILE_SIZE))
      executeSQLCommand(subCommand)

  # Create Data Files
  tableSpaceCreated = False
  for fileIndex in range(0, DATAFILES_PER_DISK):
    for disk in DATA_DISKS:
      if disk and not disk.endswith('/'):
        disk+="/" 

      if tableSpaceCreated == False:
        printStage("Creating Table Space")
        tableSpaceCreated = True
        subCommand = ("CREATE TABLESPACE %s ADD datafile '%s%s_data_file_%d.dat' use LOGFILE GROUP lg_1 INITIAL_SIZE = %s  ENGINE ndbcluster" % (TS_NAME, disk, TS_NAME, fileIndex, DATAFILE_SIZE))
      else:
        subCommand = ("ALTER TABLESPACE %s ADD datafile '%s%s_data_file_%d.dat' INITIAL_SIZE = %s  ENGINE ndbcluster" % (TS_NAME, disk, TS_NAME, fileIndex, DATAFILE_SIZE))
      executeSQLCommand(subCommand)

  #Create Table
  printStage("Creating Tables")
  subCommand = ("CREATE TABLE IF NOT EXISTS hdfs_ondisk_small_file_inode_data ( inode_id bigint(20) PRIMARY KEY, data varbinary(%d) not null ) TABLESPACE %s STORAGE DISK ENGINE ndbcluster COMMENT='NDB_TABLE=READ_BACKUP=1' partition by key (\`inode_id\`)"% (ONDISK_SMALL_FILE_INODE_SIZE, TS_NAME))
  executeSQLCommand(subCommand)
  subCommand = ("CREATE TABLE IF NOT EXISTS hdfs_ondisk_medium_file_inode_data ( inode_id bigint(20) PRIMARY KEY, data varbinary(%d) not null ) TABLESPACE %s STORAGE DISK ENGINE ndbcluster COMMENT='NDB_TABLE=READ_BACKUP=1' partition by key (\`inode_id\`)"% (ONDISK_MEDIUM_FILE_INODE_SIZE, TS_NAME))
  executeSQLCommand(subCommand)
  subCommand = ("CREATE TABLE IF NOT EXISTS hdfs_ondisk_large_file_inode_data ( inode_id bigint(20), dindex int(11), data varbinary(%d) not null,  PRIMARY KEY(inode_id, dindex) ) TABLESPACE %s STORAGE DISK ENGINE ndbcluster COMMENT='NDB_TABLE=READ_BACKUP=1' partition by key (inode_id)"% (ONDISK_LARGE_FILE_INODE_SIZE, TS_NAME))
  executeSQLCommand(subCommand)



def drop():
  #Drop table 
  printStage("Dropping Tables")
  executeSQLCommand("DROP TABLE hdfs_ondisk_small_file_inode_data")
  executeSQLCommand("DROP TABLE hdfs_ondisk_medium_file_inode_data")
  executeSQLCommand("DROP TABLE hdfs_ondisk_large_file_inode_data")

  # Drop Table Space
  printStage("Dropping Table Space")
  for fileIndex in range(0, DATAFILES_PER_DISK):
    for disk in DATA_DISKS:
      if disk and not disk.endswith('/'):
        disk+="/"

      subCommand = ("ALTER TABLESPACE %s drop datafile '%s%s_data_file_%d.dat' ENGINE ndbcluster" % (TS_NAME, disk, TS_NAME, fileIndex))
      executeSQLCommand(subCommand)

  subCommand = ("DROP TABLESPACE %s ENGINE ndbcluster"%(TS_NAME))
  executeSQLCommand(subCommand)

  # drop log group
  printStage("Dropping Log File Group")
  subCommand = "DROP LOGFILE GROUP lg_1 ENGINE ndbcluster"
  executeSQLCommand(subCommand)


if __name__ == "__main__":
    if ((args.create == False and args.drop == False) or args.host == '' or args.database == '' or args.passwd == '' or args.userName == '' or args.host == '' ) :
        parser.print_help()
        exit()

    if args.create:
        create()

    elif args.drop:
        drop()
        



        

