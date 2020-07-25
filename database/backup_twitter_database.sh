#!/bin/bash
now="$(date +'%Y-%m-%d')"
mongodump --archive --db='twitter_database' | mongorestore --archive  --nsFrom='twitter_database.*' --nsTo="twitter_database_$now.*"

