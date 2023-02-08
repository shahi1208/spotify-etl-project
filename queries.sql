-- weekly 5 top songs
SELECT song_name AS WEEKLY_HITS
FROM songs_table
WHERE date::date > current_date - 7
GROUP BY 1
ORDER BY count(song_name) DESC
HAVING count(song_name) > 1
LIMIT 5;

-- all time top 5 songs
SELECT song_name AS ALL_TIME_HITS
FROM songs_table
GROUP BY 1
ORDER BY count(song_name) DESC
HAVING count(song_name) > 1
LIMIT 5;

-- all time fav artists
SELECT artist_name AS ALL_TIME_FAV_ARTIST
FROM songs_table
GROUP BY 1
ORDER BY count(artist_name) DESC
HAVING count(artist_name) > 1
LIMIT 5;