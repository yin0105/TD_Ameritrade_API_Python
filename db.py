from mysql.connector import connect, Error
from pprint import pprint
from datetime import datetime, timedelta
import time, os, sys
from os.path import join, dirname, exists
from dotenv import load_dotenv


# Read env
dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)
database_name = os.environ.get('DATABASE_NAME')
user_name = os.environ.get('USER_NAME')
db_password = os.environ.get('DB_PASSWORD')
db_backup_path = os.environ.get('DB_BACKUP_PATH')
mysql_path = os.environ.get('MYSQL_PATH')

if len(sys.argv) > 1:
    if sys.argv[1] == "-r" or sys.argv[1] == "--restore" :
        if len(sys.argv) == 2:
            print("You need to enter the date-time to be restored as a command-line parameter.")
        else :
            restore_path = db_backup_path + '\\' + sys.argv[2]
            if exists(restore_path):
                # mysql -u root -p database_name < database_name.sql
                dumpcmd = mysql_path + "\\bin\\mysqldump -u " + user_name 
                if db_password != "":
                    dumpcmd += " -p " + db_password
                dumpcmd += " " + database_name + " < " + restore_path + "\\" + database_name + ".sql"
                os.system(dumpcmd)
            else:
                print("There is no such backup file.")
        exit()

DATETIME = time.strftime('%Y%m%d-%H%M%S')
TODAYBACKUPPATH = db_backup_path + '\\' + DATETIME
 
# Checking if backup folder already exists or not. If not exists will create it.
try:
    os.stat(TODAYBACKUPPATH)
except:
    os.mkdir(TODAYBACKUPPATH)

dumpcmd = mysql_path + "\\bin\\mysqldump -u " + user_name 
if db_password != "":
    dumpcmd += " -p " + db_password
dumpcmd += " " + database_name + " > " + TODAYBACKUPPATH + "\\" + database_name + ".sql"
os.system(dumpcmd)