# Smart Thermostat - Raspberry PI

This repository accompanies the blog post [Making your own smart 'machine learning' thermostat using Arduino, AWS, HBase, Spark, Raspberry PI and XBee](http://niektemme.com/2015/08/09/smart-thermostat/). This blog post describes building and programming your own smart thermostat. 

This smart thermostat is based on three feedback loops. 
- I. The first loop is based on an Arduino directly controlling the boiler. [Smart Thermostat - Arduino Repository](https://github.com/niektemme/smarttherm-arduino)
- **II. The second feedback loop is a Raspberry PI that receives temperature data and boiler status information from the Arduino and sends instructions to the Arduino. (this repostiory)** 
- II. The third and last feedback loop is a server in the Cloud. This server uses machine learning to optimize the boiler control model that is running on the Raspberry PI. [Smart Thermostat - AWS - HBase - Spark Repository](https://github.com/niektemme/smarttherm-aws-hbase-spark)

![Smart thermostat overview - three feedback loops](https://niektemme.files.wordpress.com/2015/07/schema_loop3.png)

## Installation & Setup

### Overview
The Raspberry PI setup consists of one Python script (smarttherm.py) and one shell script (smarttherm.sh).
The shell script is used as a init.d script and allows the Python script to run as a service in the background.

### Hardware setup
The hardware setup is described in detail in the [blog post](http://niektemme.com/2015/08/09/smart-thermostat/) mentioned above. 

### Dependencies
The following Python libraries are required.

- threading - should be installed by default
- xbee - [XBee Package](https://pypi.python.org/pypi/XBee)
- serial - should be installed by default
- struct - should be installed by default
- codecs - should be installed by default
- apsw - [Another Python SQLite Wrapper](https://github.com/rogerbinns/apsw)
- sys - should be installed by default
- time - should be installed by default
- datetime - should be installed by default
- happybase - [HappyBase](http://happybase.readthedocs.org/en/latest/)
- random - should be installed by default
- logging - should be installed by default

### Python file location
The init.d script assumes the Python script is located in /usr/local/smarttherm/ . This can be modified to any location on the Raspberry PI.

### Making the Python file executable
The Python file has to be executable to run in the background. Do: sudo chmod a+x ./smarttherm.py from the script location.

### Changing the log file location
The Python script by default sets the log file to /usr/local/tempniek/18log.log This can be changed in the beginning of the Python script.

### init.d script
The init.d script, smarttherm.sh, needs to be in the /etc/init.d/ folder. In addition the .sh file needs to be executable. The Python file has to be executable to run in the background. Do: sudo chmod a+x ./smarttherm.sh from the script location.

### HBase
Uploading the sensor data and used temperature scenarios requires HBase and the HBase thrift service to be running on a server in the cloud. The [Apache HBase](http://hbase.apache.org/book.html) website has a good tutorial. 

### HBase tables
Uploading the sensor data to HBase also requires five HBase tables. These can be created with the three commands below. This assumes LZO compression is installed. All tables only have one column family.
- create 'hsensvals', {NAME => 'fd', DATA_BLOCK_ENCODING => 'FAST_DIFF', COMPRESSION => 'LZO'}
- create 'hcurtemp', {NAME => 'fd', DATA_BLOCK_ENCODING => 'FAST_DIFF', COMPRESSION => 'LZO'}
- create 'hactscenario', {NAME => 'fd', DATA_BLOCK_ENCODING => 'FAST_DIFF', COMPRESSION => 'LZO'}
- create 'husedscenario', {NAME => 'fd', DATA_BLOCK_ENCODING => 'FAST_DIFF', COMPRESSION => 'LZO'}
- create 'husedscenariotbsc', {NAME => 'fd', DATA_BLOCK_ENCODING => 'FAST_DIFF', COMPRESSION => 'LZO'}

## Running

### Autossh
Uploading the data to the HBase cloud server requires a ssh tunnel. The following command builds a tunnel that automatically rebuilds the connection if it breaks. This requires the amazon key file to be added to the .ssh folder of the user this command is executed from. This command should not be excecuted as root.
autossh -M 0 -q -f -N -o "ServerAliveInterval 60" -o "ServerAliveCountMax 3" -L 9090:localhost:9090 -i ~/.ssh/<key file> ubuntu@<server address>.amazonaws.com

### Starting the Python service
The Python service can be started with the command: sudo /etc/init.d/smarttherm.sh start

### Stopping the Python service
The Python service can be stopped with the command: sudo /etc/init.d/smarttherm.sh stop

## Acknowledgements
The code used in this project is often based on wonderful and clearly written examples written by other people. I would especially like to thank the following people (alphabetical order).

- Aravindu Sandela - bigdatahandler - http://bigdatahandler.com
- Dave - Desert Home - http://www.desert-home.com
- Lady Ada - Adafruit - http://www.adafruit.com
- Lars George - HBase definitive guide - http://www.larsgeorge.com
- Luckily Seraph Chutium - ABC Networks Blog - http://www.abcn.net
- Michael Bouvy - http://michael.bouvy.net
- Paco Nathan - O'Reilly Media - http://iber118.com/pxn/
- Robert Faludi - Digi International - http://www.faludi.com
- Roger Binns - Another Python SQLite Wrapper - https://github.com/rogerbinns/apsw/
- Stephen Phillips - The University of Southampton, IT Innovation Centre  - http://blog.scphillips.com
