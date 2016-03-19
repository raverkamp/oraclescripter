* Oracle Scripter

A simple program to script an Oracle database schema to the file system. It is
possible to script into folder and then to commit to git.
Only code objects will be scripted, no tables.
There is one file each for all private synonyms and all sequences.
For all other objects there is one file (spec and body maybe combined).
All filenames are lowercase and not quoted. So make sure that the object names
are sane.

The system is a folder which contains the necessary jar files.
Run the program like this:

java -jar oraclescripter/main.jar propfilename [connectiondesc]

where propfilename is the name of a Java properties file, connectiondesc
is a connection identifier.
It has to be of the form host:port:service,so for your typical Oracle XE 
installation it is "localhost:1521:xe".

The properties file contains further parameters which describe the scripting task.

The properties are:
directory
  the directory to where to script to

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





 