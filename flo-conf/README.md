provide function of configuration (conf).

* 1 conf method
  - 1 java system property.
      eg: "java -Dkey=value".
  - 2 system enviroment.
      eg: in shell script, "export key=value".
  - 3 file. specified by method 1 or 2.
      eg: "java -Dflo_conf_file=/path/to/conf/file"
  - 4 class-path file "flo.conf.{flo_env}" then "flo.conf"
      (file format: properties)

* 2 conf keys used by flo
  - flo_*      : key starts with flo_ is reserved
  - enviroment : flo_env
  - conf file  : flo_conf_file