# TD_Ameritrade_API_Python
## Requirements

### Python

* Version >= 3.7.0

### MySQL

### Edit .env file
   USER_NAME: user's name to connect into MySQL database

   DB_PASSWORD: user's password to connect into MySQL database

   MYSQL_PATH: the path where MySQL is installed.

   DB_BACKUP_PATH: the path where the database will be backed up.

   API_KEY: API key

   WORK_TIME: working time

   REST_TIME: rest time

## Run
### Install Dependencies
1. launch Powershell.
2. Move to project folder.
3. Run following command. 

   pip install -r requirements.txt

### Create Database
Run following command. 

  python db.py -c

### Run
Run following command. 

  python app.py
  
### Backup Database
Run following command.

  python db.py

### Restore Database
Run following command. 

  python db.py -r {date_time}
  
  ex. python db.py -r 20210216-111111



  




