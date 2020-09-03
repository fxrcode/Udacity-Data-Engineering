# Data Modeling with Postgres

## Project Directions


## Datasets
* Log Dataset: ['artist', 'auth', 'firstName', 'gender', 'itemInSession', 'lastName',
       'length', 'level', 'location', 'method', 'page', 'registration',
       'sessionId', 'song', 'status', 'ts', 'userAgent', 'userId']
* Song Dataset: ['artist_id', 'artist_latitude', 'artist_location', 'artist_longitude',
       'artist_name', 'duration', 'num_songs', 'song_id', 'title', 'year']

## Local environment setup
* I use docker-compose to launch postgres, and cassandra.

## Datasets
* due to jupyter lab's whatever limit, I can't download data folder from workspace.
* as Himanshu's answer in: https://knowledge.udacity.com/questions/300378#300404, simply did `zip -r output.zip data`. So I can download this zip file.
* however, right click on the file shows chrome menu rather jupyterlab menu, as suggested in SOF: https://stackoverflow.com/questions/54915250/clicking-does-not-work-in-jupyterlab-os-x-chrome, it's due to extension: `Right-to-Copy`. I disabled it and right clicks works now!