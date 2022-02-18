CREATE VIEW USER_TWEET_ATTRIBUTES AS 
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





SELECT * FROM USER_TWEET_ATTRIBUTES

/*
user_id	username	tweet_count	max_tweet_length	min_tweet_length
1314070908982632448	ragrau2	2	280	280
1278776467392954368	joshlehr4	2	254	112
1493380581194932227	vee97594531	2	124	102
1465136861056282633	eag19691	2	227	227
31268233	_chrisroberts	2	170	82
28390598	23abcnews	2	246	222

*/