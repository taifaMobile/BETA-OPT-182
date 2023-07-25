# BETA-OPT-182


# BULK SMS: bulk SMS, subscribe manage, subscription sms

## Prerequisites

- Ubuntu 20+ installed in your server.
- Existing MySQL Database

## Requirements

This project requires the following dependencies to be installed:
We are using rabbitMQ here

- Python 3
- pip3
- RabbitMQ
- MySQL Database
- Celery
- Requests
- PyMySQL
- Redis
- Eventlet

## Installation

### 1. Install Python 3, pip3 & redis: <br>
```bash
sudo apt update
```

```bash
sudo apt install python3 #If you don't have python3
```

```bash
sudo apt install python3-pip
```

```bash
sudo apt install redis -y
```

check if redis service is running <br>
```bash
sudo systemctl status redis
```

### 2. Install RabbitMQ
- Go to official RabbitMQ site 

### 3. Install python3 packages 
```bash 
pip3 install celery requests pymysql redis eventlet
```
### 4. Clone this repository to your /opt folder

### 5. Assign permissions to your scripts

```bash
cd /opt
```

```bash
sudo chmod +x ./add_to_rabbit.py
```

```bash
sudo chmod +x ./pick_from_redis.py
```

```bash
sudo chmod +x ./init_subscribe_manage.py
```


### 6. Create systemd services
### (i) add_to_rabbit service
```bash
sudo nano etc/systemd/system/add_to_rabbit.service
```

paste the code below to add_to_rabbit.service file
```bash
[Unit]
Description=Add sms to rabbit service
After=multi-user.target
[Service]
Type=simple
Restart=always
ExecStart=/usr/bin/python3 /opt/add_to_rabbit.py
[Install]
WantedBy=multi-user.target 
```
save and close the file

- ##### To enable your service on every reboot
```bash
sudo systemctl enable add_to_rabbit.service
```

- ##### Reload the service files to include the new service.
```bash
sudo systemctl daemon-reload
```

- ##### Start your service
```bash
sudo service add_to_rabbit start
```

- ##### To check the status of your service
```bash
sudo service add_to_rabbit status
```

- ##### To disable your service on every reboot
```bash
sudo systemctl disable add_to_rabbit.service
```

- ##### To stop your service 
```bash
sudo service add_to_rabbit stop
```
<br><br><br>


### (ii) pick_from_redis service
```bash
sudo nano etc/systemd/system/pick_from_redis.service
```

paste the code below to pick_from_redis.service file
```bash
[Unit]
Description=Pick phone numbers from redis
After=multi-user.target
[Service]
Type=simple
Restart=always
ExecStart=/usr/bin/python3 /opt/pick_from_redis.py
[Install]
WantedBy=multi-user.target 
```
save and close the file

- ##### To enable your service on every reboot
```bash
sudo systemctl enable pick_from_redis.service
```

- ##### Reload the service files to include the new service.
```bash
sudo systemctl daemon-reload
```

- ##### Start your service
```bash
sudo service pick_from_redis start
```

- ##### To check the status of your service
```bash
sudo service pick_from_redis status
```

- ##### To disable your service on every reboot
```bash
sudo systemctl disable pick_from_redis.service
```

- ##### To stop your service 
```bash
sudo service pick_from_redis stop
```
<br><br><br>

### (iii) init_subscribe_manage service
```bash
sudo nano etc/systemd/system/init_subscribe_manage.service
```

paste the code below to pick_from_redis.service file
```bash
[Unit]
Description=Bill subsribe manage users
After=multi-user.target
[Service]
Type=simple
Restart=always
ExecStart=/usr/bin/python3 /opt/init_subscribe_manage.py
[Install]
WantedBy=multi-user.target 
```
save and close the file

- ##### To enable your service on every reboot
```bash
sudo systemctl enable init_subscribe_manage.service
```

- ##### Reload the service files to include the new service.
```bash
sudo systemctl daemon-reload
```

- ##### Start your service
```bash
sudo service init_subscribe_manage start
```

- ##### To check the status of your service
```bash
sudo service init_subscribe_manage status
```

- ##### To disable your service on every reboot
```bash
sudo systemctl disable init_subscribe_manage.service
```

- ##### To stop your service 
```bash
sudo service init_subscribe_manage stop
```

### 7. Create rescue scripts <br>
Change directory to home 

```bash
cd ~
```

### (a) launch-crashed-services
```bash
sudo nano launch-crashed-services.py
```

- Paste the code below
```python
#!/usr/bin/python3
import subprocess

# If your shell script has shebang, you can omit shell=True argument
print(subprocess.run(["/root/launch-crashed-services.sh",""], shell=True))
```
- save and close the file
<br><br>

```bash
sudo nano launch-crashed-services.sh
```


- Paste the code below
```bash
#!/bin/bash

service pick_from_redis status | grep 'active (running)' > /dev/null 2>&1
if [ $? != 0 ]
then
sudo service pick_from_redis restart > /dev/null
fi

service add_to_rabbit status | grep 'active (running)' > /dev/null 2>&1
if [ $? != 0 ]
then
sudo service add_to_rabbit restart > /dev/null
fi

service init_subscribe_manage status | grep 'active (running)' > /dev/null 2>&1
if [ $? != 0 ]
then
sudo service init_subscribe_manage restart > /dev/null
fi
```
- save and close the file

### (b) permissions
```bash
sudo nano permissions.py
```
<br>
- Paste the code below

```python 
#!/usr/bin/python3

import subprocess

# if your shell script has shebang, you can omit shell=True argument.
print(subprocess.run(["/root/permissions.sh",""], shell=True))
```
- save and close the file
<br><br>

```bash
sudo nano permissions.sh
```

- Paste the code below

```bash
#!/bin/bash

FOLDER="/opt"
sudo chmod -R 755 "${FOLDER}"
```
- save and close the file
<br>

```bash
sudo chmod +x ./launch-crashed-services.py
```

```bash
sudo chmod +x ./launch-crashed-services.sh
```

```bash
sudo chmod +x ./permissions.py
```

```bash
sudo chmod +x ./permissions.sh
```

<br>    

### (c) Create cronjobs <br>
```bash
sudo crontab -e
```

- Paste the following code to your crontab

```bash 
@reboot /opt/start_up_scripts.sh
*/15 * * * * python3 /root/launch-crashed-services.py 2> /tmp/launcher_log.txt 2>&1
*/30 * * * * python3 /root/permissions.py 2> /tmp/permissions.txt 2>&1
```
- save and close the file

## NOTE
For subscribe manage to run, you need to add the following line to your crontab in beta 1 (XXX.XXX.XXX.184)
```bash
0 */2 * * * php /var/www/html/public/sms/billCron.php >/dev/null 2>&1
```
## Usage
### To run the project:

### 1. Change directory to opt folder 

```bash
cd /opt
```

### 2. Permissions

```bash
sudo chmod +x ./add_to_rabbit.py
```

```bash
sudo chmod +x ./pick_from_redis.py
```

```bash
sudo chmod +x ./start_up_scripts.sh
```

```bash
sudo chmod +x ./start_workers.sh
```

### 3. To start the sevices 

```bash
sudo service add_to_rabbit start
```
```bash
sudo service init_subscribe_manage start
```

```bash
sudo service pick_from_redis start
```

### 4. To stop the services

```bash
sudo service add_to_rabbit stop
```

```bash
sudo service init_subscribe_manage stop
```
```bash
sudo service pick_from_redis stop
```

### 5. To check if the services are running

```bash
sudo service add_to_rabbit status
```
```bash
sudo service init_subscribe_manage status
```

```bash
sudo service pick_from_redis status
```

### 6. Run tasks.py
```bash
./start_up_scripts.sh
```

### 7. Monitoring the tasks
open another terminal
```bash
screen -r
```
<br><br>


## Conclusion

Congratulations! You have successfully installed the SMS Module

## License

[MIT License](LICENSE.md)

