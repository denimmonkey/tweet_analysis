import twint
import datetime
since = datetime.datetime(2022, 2, 12)
until = datetime.datetime(2022, 2, 18)
print(since, until)

#configuration
config = twint.Config()
config.Search = "Pandemic"
config.Lang = "en"
config.Limit = 1000
config.Since = str(since)
config.Until = str(until)
config.Store_json = True
config.Output = "raw_tweets/custom_out.json"
#running search
twint.run.Search(config)