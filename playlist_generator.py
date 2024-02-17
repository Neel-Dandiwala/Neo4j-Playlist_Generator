import spotipy
from neo4j import GraphDatabase
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import pandas as pd
from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOAuth


def generate_graph():
    neo4j = create_neo4j_session(url=neo4j_url)