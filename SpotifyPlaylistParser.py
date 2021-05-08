
import json, sqlite3

conn = sqlite3.connect("SpotifyDB.sqlite")
cur = conn.cursor()
cur.executescript('''
DROP TABLE IF EXISTS Playlist;
DROP TABLE IF EXISTS Contains;
DROP TABLE IF EXISTS Track;
DROP TABLE IF EXISTS Artist;
DROP TABLE IF EXISTS Album;

CREATE TABLE Playlist(
    id INTEGER NOT NULL UNIQUE PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL
);
CREATE TABLE Contains(
    playlist_id INTEGER,
    track_id INTEGER,
    PRIMARY KEY(playlist_id, track_id)
);
CREATE TABLE Track(
    id INTEGER NOT NULL UNIQUE PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    artist_id INTEGER NOT NULL,
    album_id INTEGER NOT NULL
);
CREATE TABLE Artist(
    id INTEGER NOT NULL UNIQUE PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL
);
CREATE TABLE Album(
    id INTEGER NOT NULL UNIQUE PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL
);
''')

def get_primary_key(table, column, logicalKey): # Retrieves a primary key from a table given a logical key and the column of the logical keys. If the table doesn't contain that logical key then it's added. 
    cur.execute("INSERT OR IGNORE INTO %s (%s) VALUES (?)" %(table, column), (logicalKey,))
    cur.execute("SELECT id FROM %s WHERE %s = ?" %(table, column), (logicalKey,))
    primaryKey = cur.fetchone()[0]
    return primaryKey


#_________________________________________________________________________________________________________



jsonPath = "MyData/Playlist1.json" #This is where the json path exported from spotify is stored.
fh = open(jsonPath)
jData = json.loads(fh.read())
playlists = jData["playlists"]

for playlist in playlists:
    playlistName = playlist["name"]
    playlistID = get_primary_key("Playlist", "name", playlistName)
    for item in playlist["items"]:
        if item["track"] == None: continue
        track = item["track"]
        trackName = track["trackName"]
        albumName = track["albumName"]
        artistName = track["artistName"]
        albumID = get_primary_key("Album", "name", albumName)
        artistID = get_primary_key("Artist", "name", artistName)
        cur.execute("INSERT OR IGNORE INTO Track (name, artist_id, album_id) VALUES (?, ?, ?)", (trackName, artistID, albumID))
        cur.execute("SELECT id FROM Track WHERE name = ?", (trackName,))
        trackID = cur.fetchone()[0]
        cur.execute("INSERT OR IGNORE INTO Contains (playlist_id, track_id) VALUES (?, ?)", (playlistID, trackID))

conn.commit()
