import os
from dotenv import load_dotenv
import spotipy
from neo4j import GraphDatabase
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import pandas as pd
from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOAuth

load_dotenv()

spotify = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials(client_id=os.getenv("SPOTIPY_CLIENT_ID"), client_secret=os.getenv("SPOTIPY_CLIENT_SECRET")))
playlist_limit = 100

def load_graph():
    neo4j = initiate_neo4j_session(url=os.getenv("NEO4J_URL"), username=os.getenv("NEO4J_USERNAME"), password=os.getenv("NEO4J_PASSWORD"))
    print("Constraint Creation")
    create_constraints(neo4j)
    
    print("Create Songs")
    songs = get_songs()
    songs = get_song_audio_features(songs)
    neo4j.run("UNWIND $tracks as track CREATE (t:Track {id: track.id}) SET t = track",
            parameters={'tracks': list(songs.values())})
    
    print("Create Albums")
    albums = get_album_info(songs)
    neo4j.run("UNWIND $albums as album CREATE (a:Album {id: album.id}) SET a = album",
            parameters={'albums': list(albums.values())})
    
    print("Create Artists")
    artists = get_artist_info(songs)
    neo4j.run("UNWIND $artists as artist CREATE (a:Artist {id: artist.id}) SET a = artist",
            parameters={'artists': list(artists.values())})
    
    print("Create Genres")
    genres = get_genres(albums, artists)
    neo4j.run("UNWIND $genres as genre MERGE (g:Genre {name: genre})",
            parameters={'genres': list(genres)})
    
    print("Linking tracks to albums, genres, and artists")
    neo4j.run("MATCH (t:Track), (a:Album {id: t.album}) CREATE (t)-[:IN_ALBUM]->(a);")
    neo4j.run("MATCH (t:Track) UNWIND t.artists as artist MATCH (a:Artist {id: artist}) CREATE (t)-[:HAS_ARTIST]->(a)")
    neo4j.run("MATCH (a:Artist) UNWIND a.genres as genre MATCH (g:Genre{name: genre}) CREATE (a)-[:HAS_GENRE]->(g)")
    
    print("Clustering Genre using GDS to create Super-Genre")
    cluster_genres(neo4j)

def create_constraints(neo4j):
    res = neo4j.run("SHOW CONSTRAINTS")
    
    for constraint in res:
        # print(constraint)
        # res = neo4j.run("DROP "+ constraint['labelsOrTypes'][0])
        res = neo4j.run("DROP CONSTRAINT " + constraint['name'])
        
    neo4j.run("CREATE CONSTRAINT FOR (g:Genre) REQUIRE g.name IS UNIQUE")
    neo4j.run("CREATE CONSTRAINT FOR (p:Playlist) REQUIRE p.name IS UNIQUE")
    neo4j.run("CREATE CONSTRAINT FOR (a:Album) REQUIRE a.id IS UNIQUE")
    neo4j.run("CREATE CONSTRAINT FOR (s:SuperGenre) REQUIRE s.id IS UNIQUE")
    neo4j.run("CREATE CONSTRAINT FOR (a:Artist) REQUIRE a.id IS UNIQUE")
    neo4j.run("CREATE CONSTRAINT FOR (t:Track) REQUIRE t.id IS UNIQUE")
    neo4j.run("MATCH (n) DETACH DELETE n;")

def cluster_genres(neo4j):
    res = neo4j.run("""
            CALL gds.graph.exists($name) YIELD exists WHERE exists CALL gds.graph.drop($name) YIELD graphName
            RETURN graphName + " was dropped." as message
            """, name='genre-has-artist')
    
    res = neo4j.run("""
            CALL gds.graph.exists($name) YIELD exists WHERE exists CALL gds.graph.drop($name) YIELD graphName
            RETURN graphName + " was dropped." as message
            """, name='genre-similar-to-genre')
    
    res = neo4j.run("""
            CALL gds.graph.project.cypher(
                'genre-has-artist',
                'MATCH (p) WHERE p:Artist OR p:Genre RETURN id(p) as id',
                'MATCH (a:Artist)-[:HAS_GENRE]->(g:Genre) RETURN id(g) AS source, id(a) AS target')
            """)
    
    res = neo4j.run("""
            CALL gds.nodeSimilarity.write('genre-has-artist', {
                writeRelationshipType: 'SIMILAR_TO',
                writeProperty: 'score'
            })
            """)
    
    res = neo4j.run("""
            CALL gds.graph.project(
                'genre-similar-to-genre',
                'Genre', {SIMILAR_TO: {orientation: 'NATURAL'}},
                {relationshipProperties:'score'}
            )
            """)
    
    res = neo4j.run("""
            CALL gds.louvain.write('genre-similar-to-genre',
            {relationshipWeightProperty: 'score', writeProperty: 'community'})
            """)
    
    # Post-processing
    res = neo4j.run("""
            MATCH (g:Genre)<-[:HAS_GENRE]-(a:Artist)<-[:HAS_ARTIST]-(t:Track)
            WITH g.community as community, collect(g) as genres, count(DISTINCT t) as trackCount
            WHERE trackCount < 10 
            UNWIND genres as g
            SET g.community = -1
            """)
    
    # Super Genre
    neo4j.run("""
            MATCH (g:Genre)
            WITH DISTINCT g.community as community
            CREATE (s:SuperGenre {id: community})
            WITH s
            MATCH (g:Genre {community: s.id})
            CREATE (g)-[:PART_OF]->(s)
            """)
    
    neo4j.run("""
            MATCH (t:Track)-[:HAS_ARTIST]->()-[:HAS_GENRE]->()-[:PART_OF]->(s:SuperGenre)
            WITH DISTINCT t,s
            CREATE (t)-[:HAS_SUPER_GENRE]->(s)
            """)
    
    neo4j.run("""
            MATCH (s:SuperGenre)--(t:Track)
            WITH s, avg(t.valence) as valence, avg(t.energy) as energy
            SET s.valence = valence, s.energy = energy
            """)

def generate_graph():
    neo4j = initiate_neo4j_session(url=os.getenv("NEO4J_URL"), username=os.getenv("NEO4J_USERNAME"), password=os.getenv("NEO4J_PASSWORD"))
    
def initiate_neo4j_session(url, username, password):
    driver = GraphDatabase.driver(url, auth=(username, password))
    return driver.session()

def get_songs():
    songs = spotify.playlist(os.getenv("SPOTIFY_PLAYLIST_URL"))['tracks']
    results = {}
    while songs['next'] or songs['previous'] is None:
        for song in songs['items']:
            # print(song)
            if song['track']['id']:
                song['track']['artists'] = [artist if type(artist) == str else artist['id'] for artist in song['track']['artists']]
                song['track']['album'] = song['track']['album'] if type(song['track']['album']) == str else song['track']['album']['id']
                results[song['track']['id']] = song['track']
            for field in song['track']:
                if song is not None and type(song['track'][field]) == dict:
                    song['track'][field] = None
        if not songs['next']:
            break
            songs = spotify.next(songs)
    # print(results)
    return results

def get_song_audio_features(songs, page_size=100):
    page_count = len(songs) / page_size
    for i in range(int(page_count) + 1):
        song_ids = list(songs.keys())[i * page_size:(i + 1) * page_size]
        if len(song_ids) == 0:
            break
        audio_features = spotify.audio_features(tracks=song_ids)
        for song_features in audio_features:
            if song_features is None:
                continue
            song_id = song_features['id']
            for feature, value in song_features.items():
                if feature != 'type':
                    songs[song_id][feature] = value
    # print(songs)
    return songs

def get_album_info(songs, page_size=20):
    album_ids = set()
    for track_id in songs.keys():
        album_ids.add(songs[track_id]['album'])
        
    all_albums = {}
    page_count = len(album_ids) / page_size
    for i in range(int(page_count) + 1):
        song_ids = list(album_ids)[i * page_size:(i + 1) * page_size]
        albums = spotify.albums(song_ids)
        
        for album in albums['albums']:
            album['artists'] = [artist['id'] for artist in album['artists']]
            album['images'] = album['images'][1]['url']
            album['external_ids'] = None
            album['external_urls'] = None
            album['tracks'] = len(album['tracks'])
            album['copyrights'] = len(album['copyrights'])
            all_albums[album['id']] = album
    return all_albums

def get_artist_info(items, page_size=50):
    all_artists = {}
    artist_ids = set()
    for song_id in items.keys():
        for artist_nr in items[song_id]['artists']:
            artist_id = artist_nr
            artist_ids.add(artist_id)
    page_count = len(artist_ids) / page_size
    for i in range(int(page_count) + 1):
        song_ids = list(artist_ids)[i * page_size:(i + 1) * page_size]
        res = spotify.artists(song_ids)
        for artist in res['artists']:
            if artist['images']:
                artist['images'] = artist['images'][1]['url']
            artist['followers'] = artist['followers']['total']
            artist['external_urls'] = None
            all_artists[artist['id']] = artist
    return all_artists

def get_genres(albums, artists):
    genres = set()
    for item in albums:
        for genre in albums[item]['genres']:
            genres.add(genre)
    for item in artists:
        for genre in artists[item]['genres']:
            genres.add(genre)
    return genres

def generate_playlist(neo4j):
    res = neo4j.run("""MATCH (p:Playlist) DETACH DELETE p""").data()
    res = neo4j.run("""MATCH (s:SuperGenre)--(t:Track) RETURN s.id, count(t) as count""").data()
    big_super_genres = [x['s.id'] for x in res if x['count'] >= playlist_limit]
    for super_genre in big_super_genres:
        make_playlist_for_big(neo4j, super_genre_id=super_genre)


def make_playlist_for_big(neo4j, super_genre_id=698):
    res = neo4j.run("""
        MATCH (s.SuperGenre{id: $superGenre})--(t:Track)
        RETURN t.id, t.danceability, t.valence as valence, t.energy as energy
    """, parameters={'superGenre': super_genre_id}).data()
    x = pd.DataFrame.from_records(result)
    
    kmeans = KMeans(n_clusters=int(len(result) / playlist_limit) + 1, random_state=0).fit(
        x[['energy', 'valence']]
    )
    if plot_kmeans_clusters:
        plt.scatter(x['energy'], x['valence'], c=kmeans.labels_, s=50, cmap='viridis')
        plt.show()
    x['label'] = kmeans.labels_
    
    output = x[['t.id', 'label']].values.tolist()
    neo4j.run("""
        UNWIND $output as row
        MATCH (s:SuperGenre{id: $superGenre})--(t:Track{id: row[0]})
        MERGE (p:Playlist{id: $superGenre + "-" + row[1]})
        SET p.energy = $centers[row[1]][0]
        SET p.valence = $centers[row[1]][1]
        CREATE (t)-[:IN_PLAYLIST]->(p)
    """, parameters={'output':output, 'superGenre':super_genre_id, 'ceneters':kmeans.cluster_centers_.tolist()})

def create_playlist(neo4j, page_size=100):
    res = neo4j.run("""
        MATCH (n:Playlist)-[:IN_PLAYLIST]-(t:Track)
        RETURN n.name as name, n.valence as valence, n.energy as energy, n.id as playlist, collect(t.id) as tracks
    """).data()
    print(res)
    

if __name__=='__main__':
    load_graph()
    