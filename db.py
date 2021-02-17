from mysql.connector import connect, Error
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
                dumpcmd = mysql_path + "\\bin\\mysql -u " + user_name 
                if db_password != "":
                    dumpcmd += " -p " + db_password
                dumpcmd += " " + database_name + " < " + restore_path + "\\" + database_name + ".sql"
                os.system(dumpcmd)
            else:
                print("There is no such backup file.")
        exit()
    elif sys.argv[1] == "-c" or sys.argv[1] == "--create" :
        try:
            conn = connect(host='localhost',
                            database='mysql',
                            user=user_name,
                            password=db_password)
            if conn.is_connected():
                print('Creating MySQL database . . .')
                try:
                    query = """DROP DATABASE IF EXISTS `ameritrade`
                            CREATE DATABASE  `ameritrade`
                            DROP TABLE IF EXISTS `ameritrade`.`instruments`;
                            CREATE TABLE `ameritrade`.`instruments` (`id` bigint(20) NOT NULL AUTO_INCREMENT,  `symbol` varchar(10) NOT NULL,  `assetType` varchar(10) NOT NULL,  `cusip` varchar(20) NOT NULL,  `description` text NOT NULL,  `exchange` varchar(20) NOT NULL,  `beta` float NOT NULL,  `bookValuePerShare` float NOT NULL,  `currentRatio` float NOT NULL,  `divGrowthRate3Year` float NOT NULL,  `dividendAmount` float NOT NULL,  `dividendDate` datetime NOT NULL,  `dividendPayAmount` float NOT NULL,  `dividendPayDate` datetime NOT NULL,  `dividendYield` float NOT NULL,  `epsChange` float NOT NULL,  `epsChangePercentTTM` float NOT NULL,  `epsChangeYear` float NOT NULL,  `epsTTM` float NOT NULL,  `grossMarginMRQ` float NOT NULL,  `grossMarginTTM` float NOT NULL,  `high52` float NOT NULL,  `interestCoverage` float NOT NULL,  `low52` float NOT NULL,  `ltDebtToEquity` float NOT NULL,  `marketCap` float NOT NULL,  `marketCapFloat` float NOT NULL,  `netProfitMarginMRQ` float NOT NULL,  `netProfitMarginTTM` float NOT NULL,  `operatingMarginMRQ` float NOT NULL,  `operatingMarginTTM` float NOT NULL,  `pbRatio` float NOT NULL,  `pcfRatio` float NOT NULL,  `peRatio` float NOT NULL,  `pegRatio` float NOT NULL,  `prRatio` float NOT NULL,  `quickRatio` float NOT NULL,  `returnOnAssets` float NOT NULL,  `returnOnEquity` float NOT NULL,  `returnOnInvestment` float NOT NULL,  `revChangeIn` float NOT NULL,  `revChangeTTM` float NOT NULL,  `revChangeYear` float NOT NULL,  `sharesOutstanding` float NOT NULL,  `shortIntDayToCover` float NOT NULL,  `shortIntToFloat` float NOT NULL,  `totalDebtToCapital` float NOT NULL,  `totalDebtToEquity` float NOT NULL,  `vol10DayAvg` float NOT NULL,  `vol1DayAvg` float NOT NULL,  `vol3MonthAvg` float NOT NULL,  `fetchedDate` datetime NOT NULL,  PRIMARY KEY (`id`)) ENGINE=InnoDB AUTO_INCREMENT=26 DEFAULT CHARSET=utf8mb4;
                            DROP TABLE IF EXISTS `ameritrade`.`price_history`;
                            CREATE TABLE `ameritrade`.`price_history` (  `id` bigint(20) NOT NULL AUTO_INCREMENT,  `symbol` varchar(20) NOT NULL,  `empty` tinyint(1) NOT NULL,  `close` float NOT NULL,  `date_time` datetime NOT NULL,  `high` float NOT NULL,  `low` float NOT NULL,  `open` float NOT NULL,  `volume` bigint(20) NOT NULL,  `fetchedDate` datetime NOT NULL,  PRIMARY KEY (`id`)) ENGINE=InnoDB AUTO_INCREMENT=1548 DEFAULT CHARSET=utf8mb4;
                            DROP TABLE IF EXISTS `ameritrade`.`quotes`;
                            CREATE TABLE `ameritrade`.`quotes` (  `id` bigint(20) NOT NULL AUTO_INCREMENT,  `symbol` varchar(20) NOT NULL,  `52WkHigh` float NOT NULL,  `52WkLow` float NOT NULL,  `askId` varchar(10) NOT NULL,  `askPrice` float NOT NULL,  `askSize` bigint(20) NOT NULL,  `assetMainType` varchar(20) NOT NULL,  `assetType` varchar(20) NOT NULL,  `bidId` varchar(10) NOT NULL,  `bidPrice` float NOT NULL,  `bidSize` bigint(20) NOT NULL,  `bidTick` varchar(10) NOT NULL,  `closePrice` float NOT NULL,  `cusip` varchar(20) NOT NULL,  `delayed_` tinyint(1) NOT NULL,  `description` text NOT NULL,  `digits` int(11) NOT NULL,  `divAmount` float NOT NULL,  `divDate` datetime NOT NULL,  `divYield` float NOT NULL,  `exchange` varchar(10) NOT NULL,  `exchangeName` varchar(20) NOT NULL,  `highPrice` float NOT NULL,  `lastId` varchar(10) NOT NULL,  `lastPrice` float NOT NULL,  `lastSize` bigint(20) NOT NULL,  `lowPrice` float NOT NULL,  `marginable` bigint(20) NOT NULL,  `mark` float NOT NULL,  `markChangeInDouble` float NOT NULL,  `markPercentChangeInDouble` float NOT NULL,  `nAV` float NOT NULL,  `netChange` float NOT NULL,  `netPercentChangeInDouble` float NOT NULL,  `openPrice` float NOT NULL,  `peRatio` float NOT NULL,  `quoteTimeInLong` bigint(20) NOT NULL,  `regularMarketLastPrice` float NOT NULL,  `regularMarketLastSize` bigint(20) NOT NULL,  `regularMarketNetChange` float NOT NULL,  `regularMarketPercentChangeInDouble` float NOT NULL,  `regularMarketTradeTimeInLong` bigint(20) NOT NULL,  `securityStatus` varchar(20) NOT NULL,  `shortable` tinyint(1) NOT NULL,  `totalVolume` bigint(20) NOT NULL,  `tradeTimeInLong` bigint(20) NOT NULL,  `volatility` float NOT NULL,  `fetchedDate` datetime NOT NULL,  PRIMARY KEY (`id`)) ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=utf8mb4;
                            """
                    cursor = conn.cursor()
                    for row in query.splitlines() :
                        cursor.execute(row)
                        conn.commit()
                except :
                    print("Failed to create database")
                    exit()
            else:
                print("Warnig: Can't connect to MySQL database")

        except Error as e:
            print("Warnig: Can't connect to MySQL database")
            print(e)
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