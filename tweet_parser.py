import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy 
import matplotlib.pyplot as plt
import os

#from nltk.corpus import stopwords
#stop = stopwords.words('english')

stopwords_english = ['the', 'pandemic', 'to', 'a', 'of', 'and', 'in', 'i', 'is', 'for', 'that', 'this', 'it', 'you', 'we', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', "you're", "you've", "you'll", "you'd", 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', "she's", 'her', 'hers', 'herself', 'it', "it's", 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', "that'll", 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 'can', 'will', 'just', 'don', "don't", 'should', "should've", 'now', 'ain', 'aren', "aren't", 'couldn', "couldn't", 'didn', "didn't", 'doesn', "doesn't", 'hadn', "hadn't", 'hasn', "hasn't", 'haven', "haven't", 'isn', "isn't", 'mightn', "mightn't", 'mustn', "mustn't", 'needn', "needn't", 'shan', "shan't", 'shouldn', "shouldn't", 'wasn', "wasn't", 'weren', "weren't", 'won', "won't", 'wouldn', "wouldn't", "with", "including"]


def truncate_table(connection, table_name):
    """
        truncate table 
    """
    connection.execute(f"TRUNCATE TABLE {table_name};")

def execute_query(connection, query):
    """
        execute query
    """
    connection.execute(query)

def create_plots(top_n, df, plot_name):
    plt.bar(df.head(top_n).word, df.head(top_n).frequency)
    plt.savefig(f"plots/top_{plot_name}_{top_n}.png")
    plt.clf()

def horizontal_bar_plot(top_n, df, plot_name):
    fig, ax = plt.subplots(figsize=(8, 8))

    # Plot horizontal bar graph
    df.head(top_n).sort_values(by='frequency').plot.barh(x='word',
                          y='frequency',
                          ax=ax,
                          color="purple")

    ax.set_title("word frequency")

    plt.savefig(f"plots/{top_n}_{plot_name}.png")

raw_tweets = pd.read_json("raw_tweets/custom_out.json", lines=True)
raw_tweets.drop(columns = ['conversation_id', 'created_at', 'date', 'time', 'timezone', 'name', 'place', 'language', 'mentions', 'urls', 'photos', 'replies_count', 'retweets_count', 'likes_count', 'hashtags', 'cashtags', 'link', 'retweet', 'quote_url', 'video', 'thumbnail', 'near', 'geo', 'source', 'user_rt_id', 'user_rt', 'retweet_id', 'reply_to', 'retweet_date', 'translate', 'trans_src', 'trans_dest'], inplace =True)


raw_tweets["tweet"] = raw_tweets.tweet.str.lower()

raw_tweets = raw_tweets[raw_tweets.tweet.str.contains("pandemic")]


connection_string = os.environ.get('PG_CONNECTION_STRING')
engine = create_engine(connection_string)


dtype={'id': sqlalchemy.types.Text(), 
                   'user_id': sqlalchemy.types.Text(),
                   'username': sqlalchemy.types.VARCHAR(length=30),
                   'tweet': sqlalchemy.types.Text()}

raw_tweets.to_sql("raw_tweets", con=engine, if_exists='replace', dtype=dtype, index=False)

connection = engine.connect()

ddl_table_name = """
                    CREATE TABLE if not exists NAME (
                        user_id text,
                        username varchar(15),
                        id text
                    );
                """

insert_into_name = "INSERT INTO NAME SELECT user_id, username, id FROM raw_tweets;"

execute_query(connection, ddl_table_name)
truncate_table(connection, "name")
execute_query(connection, insert_into_name)


ddl_table_tweet = """CREATE TABLE if not exists TWEET (
                        id text,
                        tweet text
                        );
                """

insert_into_tweet = "INSERT INTO tweet SELECT id, tweet FROM raw_tweets;"

execute_query(connection, ddl_table_tweet)
truncate_table(connection, "tweet")
execute_query(connection, insert_into_tweet)



ddl_table_length = """
                    CREATE TABLE if not exists length (
                    id text,
                    length int
                    );
                   """

insert_into_length = "INSERT INTO length SELECT id, length(tweet) FROM raw_tweets;"

execute_query(connection, ddl_table_length)
truncate_table(connection, "length")
execute_query(connection, insert_into_length)


create_view_query = """
                    CREATE OR REPLACE VIEW USER_TWEET_ATTRIBUTES AS 
                    SELECT 
                        NAME.USER_ID,
                        NAME.USERNAME,
                        COUNT(TWEET.ID) AS TWEET_COUNT,
                        MAX(LENGTH.LENGTH) MAX_TWEET_LENGTH,
                        MIN(LENGTH.LENGTH) MIN_TWEET_LENGTH
                    FROM 
                        NAME JOIN TWEET ON NAME.ID = TWEET.ID
                    JOIN LENGTH ON TWEET.ID = LENGTH.ID
                    GROUP BY NAME.USER_ID, NAME.USERNAME
                    ORDER BY COUNT(TWEET.ID) DESC;
                    """

execute_query(connection, create_view_query)

wordcount_df = raw_tweets.tweet.str.replace('[^\w\s]','').str.split(expand=True).stack().value_counts().reset_index()
wordcount_df = wordcount_df.rename(columns={'index':'word', 0:'frequency'})
wordcount_df.sort_values('frequency', ascending=False)
wordcount_df.to_sql("frequency", con=engine, if_exists='replace', dtype=dtype, index=False)



create_plots(10,wordcount_df, "all_words")
create_plots(30,wordcount_df, "all_words")
create_plots(50,wordcount_df, "all_words")

horizontal_bar_plot(10, wordcount_df, "all_words")
horizontal_bar_plot(30, wordcount_df, "all_words")
horizontal_bar_plot(50, wordcount_df, "all_words")


wordcount_df_stop_words_removed = wordcount_df[~wordcount_df['word'].isin(stopwords_english)]


create_plots(10,wordcount_df, "without_stop_words")
create_plots(30,wordcount_df, "without_stop_words")
create_plots(50,wordcount_df, "without_stop_words")

horizontal_bar_plot(10, wordcount_df_stop_words_removed, "without_stop_words")
horizontal_bar_plot(30, wordcount_df_stop_words_removed, "without_stop_words")
horizontal_bar_plot(50, wordcount_df_stop_words_removed, "without_stop_words")