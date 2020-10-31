import csv
import datetime
import pprint
from pymongo import MongoClient

# CSV to JSON Conversion
csvfile = open('./crypto_historical_data.csv', 'r')
reader = csv.DictReader(csvfile)
mongo_uri = "mongodb+srv://<username>:<password>@utu-code-test.3faqs.mongodb.net/utu_code_test?retryWrites=true&w=majority"
mongo_client = MongoClient(mongo_uri)
db = mongo_client["utu_code_test"]
collection = db["crypto_historical_data"]
collection.delete_many({})
# headers = ["Currency", "Date", "Open", "High", "Low", "Close", "Volume", "Market Cap"]
# keys = ["currency", "date", "open", "high", "low", "close", "volume", "marketCap"]
# types = ["str", "date", "float", "float", "float", "float", "float", "float"]

for each in reader:
  row = {}

  row["currency"] = each["Currency"]
  row["date"] = datetime.datetime.strptime(each["Date"], "%b %d, %Y")
  row["open"] = float(each["Open"].replace(',', ''))
  row["high"] = float(each["High"].replace(',', ''))
  row["low"] = float(each["Low"].replace(',', ''))
  row["close"] = float(each["Close"].replace(',', ''))
  row["volume"] = float(each["Volume"].replace(',', ''))
  row["marketCap"] = float(each["Market Cap"].replace(',', ''))

  data_id = collection.insert_one(row).inserted_id
  pprint.pprint(data_id)
