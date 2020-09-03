# Data Modeling with Postgres

## Project Directions


## Datasets
* Log Dataset: ['artist', 'auth', 'firstName', 'gender', 'itemInSession', 'lastName',
       'length', 'level', 'location', 'method', 'page', 'registration',
       'sessionId', 'song', 'status', 'ts', 'userAgent', 'userId']

```json
{
    "artist": "Quad City DJ's",
    "auth": "Logged In",
    "firstName": "Chloe",
    "gender": "F",
    "itemInSession": 0,
    "lastName": "Cuevas",
    "length": 451.44771,
    "level": "free",
    "location": "San Francisco-Oakland-Hayward, CA",
    "method": "PUT",
    "page": "NextSong",
    "registration": 1540940782796.0,
    "sessionId": 506,
    "song": "C'mon N' Ride It (The Train) (LP Version)",
    "status": 200,
    "ts": 1542081112796,
    "userAgent": "Mozilla\/5.0 (Windows NT 5.1; rv:31.0) Gecko\/20100101 Firefox\/31.0",
    "userId": "49"
}
```
* Song Dataset: ['artist_id', 'artist_latitude', 'artist_location', 'artist_longitude',
       'artist_name', 'duration', 'num_songs', 'song_id', 'title', 'year']

```json
{
    "num_songs": 1,
    "artist_id": "ARJIE2Y1187B994AB7",
    "artist_latitude": null,
    "artist_longitude": null,
    "artist_location": "",
    "artist_name": "Line Renaud",
    "song_id": "SOUPIRU12A6D4FA1E1",
    "title": "Der Kleine Dompfaff",
    "duration": 152.92036,
    "year": 0
}
```

## Local environment setup
* I use docker-compose to launch postgres, and cassandra.
* `docker-compose up --force-recreate` to launch
* `docker-compose down` to clean

## Datasets
* due to jupyter lab's whatever limit, I can't download data folder from workspace.
* as Himanshu's answer in: https://knowledge.udacity.com/questions/300378#300404, simply did `zip -r output.zip data`. So I can download this zip file.
* however, right click on the file shows chrome menu rather jupyterlab menu, as suggested in SOF: https://stackoverflow.com/questions/54915250/clicking-does-not-work-in-jupyterlab-os-x-chrome, it's due to extension: `Right-to-Copy`. I disabled it and right clicks works now!