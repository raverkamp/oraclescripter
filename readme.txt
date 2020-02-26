* Oracle Scripter

A simple program to script an Oracle database schema to the file system. It is
possible to script into folder and then to commit to git.
Only code objects will be scripted, no tables.
There is one file each for all private synonyms and all sequences.
For all other objects there is one file (spec and body maybe combined).
All filenames are lowercase and not quoted. So make sure that the object names
are sane.

Additionally the program is able to compare sources on disk to the database.
This mode is triggered if the first parameter is "compare". See below.

The system is a folder which contains the necessary jar files.
Run the program like this:

java -jar oraclescripter/main.jar propfilename [connectiondesc]

where propfilename is the name of a Java properties file, connectiondesc
is a connection identifier.

It has to be of the form user/password@host:port:service, so for your typical Oracle XE 
installation it is "scott/tiger@localhost:1521:xe".

If the password is omitted it has to be entered on the console.

The properties file contains further parameters which describe the scripting task.

The properties are:

connection
   connectiondesc, must be given if no conenction descriptor on commandline

schemas
  optional comma separated list of schema names
    either: for each schema name <schema>  there must be an entry <schema>.connection
    or: there is a connection entry and this connection has acces to the dba views
  each schema will get its own directory the name is <schema>

directory
  the directory to where to script to. If this is absolute path it will be taken.
  a relative Path is resolved against the directory of the properties file.
  the current directory is only used for finding the properties file.

usegit
  wether to script and then checkin via the git version control system

combine_spec_and_body
  true or false, wether to combine spec and body of packages and types in one file

private-synonyms
  true or false, wether to save private synonmys a special files will be created

sequences
  true or false, wether to script sequence, the current value will not be scripted

encoding
    the encoding for the generated files, defaults to UTF-8

objects
    comma separated list of objects to script

object-where
    where clause for select on user_objects

object-file
    a file name, this file contains one object name per line

Note: excatly one of the object clauses must be supplied.

The following type of object are known:

package_body
package_spec
package
type_body
type_spec
type
procedure
function
view
trigger

To set the suffix of the generated files use the properties
suffix.<type>
The program uses reasonable defaults. 

To set the sub directory where the files are place use properties
dir.<type>
The default is to place all files in the main directory.


NOTE:
support for triggers is thin.


The program is able to compare sources on disk to the sources in the database.
This mode is triggered if the first parameter is "compare". 
In this mode the second parameter must be a file which contains the parameters.

Run the program like this:

java -jar oraclescripter/main.jar compare filename

The file named filename must be Java properties file.


The properties are:

schemas
  a comma separated list of schema names, case invariant

connection
  a conenction desc for the database connection

for each listed schema X there must be property
X.start
  which names are SQL-Plus file which contains the source for the 
  database objects. The program tries to emulate SQL-Plus behaviour, but this
  is limited. Supported statements are "create procedure, package ..." and "start"
  respectively "@" and "@@". 
  The filename is resolved relative to the folder of the properties file.

winmerge_cmd
  if this property is set (even empty) then show the differing files with winmerge. 
  if the value is empty then a reasonbale default is chosen oetherwise use the value.

The program compares for each schema the sources defined on disk with the 
sources of the schema in the database. A temporary directory is created and the
files that are different are stored in a subdirectory "DISK" respectively "DB".
If the winmerge_cmd option is given then winmerge is started to compare these two
directories. If the option is empty, the assumption is that the winmerge binary is at:
C:\Program Files (x86)\WinMerge\WinMergeU.exe

charset
  the charset of the source files, if not supplied the default is ISO-8859-1

Obviously the user for the given schema must be able to read the sources of all
schemas, so either there is only one schema and the connection is for this
schema. Otherwise the user of the connection has the SELECT ANY DICTIONARY System Privilege
or the user has the SELECT_CATALOG_ROLE role.
