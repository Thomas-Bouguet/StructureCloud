import pymongo
import pandas as pd
import streamlit as st
from st_aggrid import AgGrid
import time
from pymongo import MongoClient
import matplotlib.pyplot as plt
import time
from pymongo import MongoClient

import pymongo
from sshtunnel import SSHTunnelForwarder
import paramiko




# Streamlit app
def main():
    page = st.sidebar.selectbox("Select a page", ["User", "Analyst","Administrator"])

    #Vue User 
    if page == "User":
        ssh_username = 'administrateur'
        ssh_password = 'SuperPassword!1'
        ssh_hostname = 'MESIIN592023-00049.westeurope.cloudapp.azure.com'
        ssh_port = 22  

        mongo_hostname = 'mesiin592023-00049'
        mongo_port = 30000  
        with SSHTunnelForwarder(
            (ssh_hostname, ssh_port),
            ssh_username=ssh_username,
            ssh_password=ssh_password,
            remote_bind_address=(mongo_hostname, mongo_port),
            ssh_pkey=None,  
            mute_exceptions=True  
        ) as tunnel:
            
            client = pymongo.MongoClient('127.0.0.1', tunnel.local_bind_port)

            #Fonction qui permet de run les mongos queries users.
            def run_mongo_query_user(query_name):
          
                db = client.myDatabase
                collection = db.movies
                
                if query_name == 'Query 1':
                    collection = db.movies
                    query = {
                    "list_genres": "Sci-Fi",
                    "rank": {
                        "$gt": 8
                    }
                }
                    projection = {
                        "name": 1,
                        "rank": 1,
                        "_id": 0 
                    }
                elif query_name == 'Query 2':
                    collection = db.directors
                    query = {
                            "dict_genres_den_prob.Romance": {
                                "$exists": True,
                                "$ne": [None, 0]
                            }
                        }

                    projection = {
                        "first_name": 1,
                        "last_name": 1,
                        "_id": 0  
                    }
                elif query_name == 'Query 3':
                    collection = db.movies
                    query = {
                            "list_genres": "Animation",
                            "year": 1995
                        }

                    projection = {
                        "name": 1,
                        "_id": 0  
                    }
                elif query_name == 'Query 4':
                    collection = db.directors
                    collection_directors = db.directors
                    collection = db.movies
                    kurosawa_movies = collection_directors.find_one(
                            {
                                "first_name": "Akira",
                                "last_name": "Kurosawa"
                            },
                            {
                                "list_movies_id": 1,
                                "_id": 0
                            }
                        )

                    kurosawa_movies_id = kurosawa_movies["list_movies_id"] if kurosawa_movies else []
                    query = {
                        "id": {
                            "$in": kurosawa_movies_id
                        },
                        "list_genres": "Drama"
                    }
                    projection = {
                        "name": 1,
                        "_id": 0  
                    }

                result = collection.find(query,projection) 
                df = pd.DataFrame(list(result)) 
                
                styled_df = df.style.applymap(lambda x: f'font-size: 20px', subset=pd.IndexSlice[:, :])

                
                st.dataframe(styled_df, height=500,width =700)
                
            st.title('User View')
            st.subheader('All the science-fiction movies with a ranking above 8. (Query 1)')
           

           #en fonction du bouton appuyé on choisi la query executé dans la fonction créer juste avant
            if st.button('Run Query 1'):
                run_mongo_query_user('Query 1')

            st.write("----")
            st.subheader('All the directors who made at least one romance movie. (Query 2)')
            if st.button('Run Query 2'):
                run_mongo_query_user('Query 2')
            st.write("----")
            st.subheader('All the gangster movies in 1995 (Query 3)')
            if st.button('Run Query 3'):
                run_mongo_query_user('Query 3')
            st.write("----")
            st.subheader('All the movies of Akira Kurosawa that are historical movies. (Query 4)')
            if st.button('Run Query 4'):
                run_mongo_query_user('Query 4')
            st.write("----")

    #Vue administrateur
    elif page == "Administrator":
        st.title('Administrator View')
        #Recuperation du nombre de doc par collection
        def count_doc():
            # Connect to MongoDB
            ssh_username = 'administrateur'
            ssh_password = 'SuperPassword!1'
            ssh_hostname = 'MESIIN592023-00049.westeurope.cloudapp.azure.com'
            ssh_port = 22  

            mongo_hostname = 'mesiin592023-00049'
            mongo_port = 30000 
            with SSHTunnelForwarder(
                (ssh_hostname, ssh_port),
                ssh_username=ssh_username,
                ssh_password=ssh_password,
                remote_bind_address=(mongo_hostname, mongo_port),
                ssh_pkey=None,
                mute_exceptions=True
            ) as tunnel:
                client = pymongo.MongoClient('127.0.0.1', tunnel.local_bind_port)
                db = client.myDatabase
                doc_count_actors = []
                doc_count_movies = []
                doc_count_directors = []
                # Get statistics of distribution of data
                stats = db.command("collStats", "actors")
                
                for shard, shard_info in stats["shards"].items(): 
                    doc_count_actors.append(shard_info["count"])

                stats = db.command("collStats", "movies")
                
                for shard, shard_info in stats["shards"].items(): 
                    doc_count_movies.append(shard_info["count"])

                stats = db.command("collStats", "directors")
                
                for shard, shard_info in stats["shards"].items(): 
                    doc_count_directors.append(shard_info["count"])

                return doc_count_actors,doc_count_movies,doc_count_directors

        #Recuperation des infos cluster (nombre replica, shards et state)
        def cluster_state():
           
            ssh_username = 'administrateur'
            ssh_password = 'SuperPassword!1'
            ssh_hostname = 'MESIIN592023-00049.westeurope.cloudapp.azure.com'
            ssh_port = 22  

            mongo_hostname = 'mesiin592023-00049'
            mongo_port = 30000 
            with SSHTunnelForwarder(
                (ssh_hostname, ssh_port),
                ssh_username=ssh_username,
                ssh_password=ssh_password,
                remote_bind_address=(mongo_hostname, mongo_port),
                ssh_pkey=None,
                mute_exceptions=True
            ) as tunnel:
                client = pymongo.MongoClient('127.0.0.1', tunnel.local_bind_port)
                db = client.admin

                cluster_info = db.command("listShards")

                
                shard_info = []
                for shard in cluster_info['shards']:
                    hosts = shard['host'].split('/')[1]  
                    unique_hosts = len(set(hosts.split(',')))  
                    print(shard["host"])
                    shard_info.append({
                        'Shard': shard['_id'],
                        'state': shard['state'],
                        'Replicaset': unique_hosts
                    })
                    

                return shard_info
                
        #Recuperation des index présent pour chaque collection
        def get_indexes():
            ssh_username = 'administrateur'
            ssh_password = 'SuperPassword!1'
            ssh_hostname = 'MESIIN592023-00049.westeurope.cloudapp.azure.com'
            ssh_port = 22 
            mongo_hostname = 'mesiin592023-00049'
            mongo_port = 30000  
            with SSHTunnelForwarder(
                (ssh_hostname, ssh_port),
                ssh_username=ssh_username,
                ssh_password=ssh_password,
                remote_bind_address=(mongo_hostname, mongo_port),
                ssh_pkey=None,
                mute_exceptions=True
            ) as tunnel:
                client = pymongo.MongoClient('127.0.0.1', tunnel.local_bind_port)
                db = client.myDatabase  
                collection = db.movies 
                indexes = collection.list_indexes()
                index_info_movies = []
                for index in indexes:
                    index_info_movies.append(index["key"])

                collection = db.actors 
                indexes = collection.list_indexes()
                index_info_actors = []
                for index in indexes:
                    index_info_actors.append(index["key"])

                collection = db.directors
                indexes = collection.list_indexes()
                index_info_directors = []
                for index in indexes:
                    index_info_directors.append(index["key"])

                return index_info_movies,index_info_directors,index_info_actors
            
        #recupération des stats sur la distribution des données pour chaque collection.
        def data_distribution_stats():
                ssh_username = 'administrateur'
                ssh_password = 'SuperPassword!1'
                ssh_hostname = 'MESIIN592023-00049.westeurope.cloudapp.azure.com'
                ssh_port = 22 
                mongo_hostname = 'mesiin592023-00049'
                mongo_port = 30000  
                with SSHTunnelForwarder(
                    (ssh_hostname, ssh_port),
                    ssh_username=ssh_username,
                    ssh_password=ssh_password,
                    remote_bind_address=(mongo_hostname, mongo_port),
                    ssh_pkey=None,
                    mute_exceptions=True
                ) as tunnel:
                    client = pymongo.MongoClient('127.0.0.1', tunnel.local_bind_port)
                    db = client.myDatabase 
                    stats_directors = db.command("collStats", "directors")
                    stats_movies = db.command("collStats", "movies")
                    stats_actors = db.command("collStats", "actors")
                    return stats_directors,stats_movies,stats_actors


        #display de toutes les informations avec les fonctions créer juste avant
        st.subheader('Statistics of Data Distribution')
        stats_directors,stats_movies,stats_actors = data_distribution_stats()
        st.write("movies collection")
        st.write(stats_movies)
        st.write("actors collection")
        st.write(stats_actors)
        st.write("directors collection")
        st.write(stats_directors)
        st.subheader('Cluster State Information')
        cluster_info = cluster_state()
        doc_count_actors,doc_count_movies,doc_count_directors = count_doc()
        for i in range(len(cluster_info)):
          cluster_info[i]["count_actors"] = doc_count_actors[i]
          cluster_info[i]["count_movies"] = doc_count_movies[i]
          cluster_info[i]["count_directors"] = doc_count_directors[i]
        st.write(cluster_info)

        st.subheader("index keys:")
        index_info_movies,index_info_directors,index_info_actors = get_indexes()
        st.write("movies indexe :")
        st.write(index_info_movies)
        st.write("directors index :")
        st.write(index_info_directors)
        st.write("actors index :")
        st.write(index_info_actors)
    
    #Vue Analyst
    elif page == "Analyst":
        st.title('Analyst View')
        ssh_username = 'administrateur'
        ssh_password = 'SuperPassword!1'
        ssh_hostname = 'MESIIN592023-00049.westeurope.cloudapp.azure.com'
        ssh_port = 22  

        mongo_hostname = 'mesiin592023-00049'
        mongo_port = 30000  
        with SSHTunnelForwarder(
            (ssh_hostname, ssh_port),
            ssh_username=ssh_username,
            ssh_password=ssh_password,
            remote_bind_address=(mongo_hostname, mongo_port),
            ssh_pkey=None,  
            mute_exceptions=True  
        ) as tunnel:
            
            client = pymongo.MongoClient('127.0.0.1', tunnel.local_bind_port)
            
            #Fonction permettant d'executer les queries complexe/analyst qui retournera pour chaque query un dataframe
            def run_mongo_query_analyst(query_name,param1,param2):
                db = client.myDatabase
                collection = db.movies
                if query_name == 'Query 1':
                    collection_directors = db.directors
                    agnes_varda = collection_directors.find_one({
                        "first_name": param1,
                        "last_name": param2
                        })

                    id_varda_movies = agnes_varda["list_movies_id"] if agnes_varda else []


                    query = [
                        {
                            "$match": {
                                "id": {"$in": id_varda_movies},
                                "rank": {
                                    '$exists': True,
                                    '$ne': float('nan')
                                }
                            }
                        },
                        {
                            "$group": {
                                "_id": None,
                                "averageRank": {"$avg": "$rank"}
                            }
                        }
                    ]
                    collection = db.movies
                elif query_name == 'Query 2':
                    collection = db.directors
                    query = [
                        {
                            "$project": {
                                "first_name": 1,
                                "last_name": 1,
                                "filteredGenres": {
                                    "$filter": {
                                        "input": {
                                            "$objectToArray": "$dict_genres_den_prob"
                                        },
                                        "as": "genre",
                                        "cond": {
                                            "$and": [
                                                {"$ne": ["$$genre.v", None]},
                                                {"$ne": ["$$genre.v", 0]}
                                            ]
                                        }
                                    }
                                }
                            }
                        },
                        {
                            "$match": {
                                "$and": [
                                    {"filteredGenres": {"$exists": True}},
                                    {"filteredGenres": {"$ne": None}},
                                    {
                                        "$expr": {
                                            "$eq": [
                                                {"$size": "$filteredGenres"},
                                                int(param1)
                                            ]
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "$project": {
                                "first_name": 1,
                                "last_name": 1
                            }
                        }
                    ]
                elif query_name == 'Query 3':
                    query = [
                        {
                            "$match": {
                                "den_nb_actors": { "$lt": int(param1) },
                                "rank": { "$gt": int(param2) }
                            }
                        },
                        {
                            "$project": {
                                "name": 1,
                                "_id": 0
                            }
                        }
                    ]

                elif query_name == 'Query 4':
                    collection_directors = db.directors
                    collection = db.movies
                    pipeline_directors = [
                        {
                            "$project": {
                                "id": 1,
                                "list_movies_id":1,
                                "full_name": {
                                    "$concat": [ "$first_name", " ", "$last_name" ]
                                }
                            }
                        },
                        {
                            "$match": {
                                "full_name": {
                                    "$in": param1
                                }
                            }
                        },
                        {
                            "$unwind": "$list_movies_id"
                        },
                        {
                            "$group": {
                                "_id": None,
                                "list_director_id": { "$push": "$list_movies_id" }
                            }
                        }
                    ]
                    directors_new_h = collection_directors.aggregate(pipeline_directors)
                    directors_new_h_array = list(directors_new_h)
                    directors_new_h_id = directors_new_h_array[0]['list_director_id'] 
                    query = [
                        {
                            "$match": {
                                "id": {
                                    "$in": directors_new_h_id
                                }
                            }
                        },
                        {
                            "$unwind": "$list_genres"
                        },
                        {
                            "$group": {
                                "_id": "$list_genres",
                                "bestRank": {
                                    "$max": {
                                        "$cond": {
                                            "if": { "$eq": ["$rank", None] },
                                            "then": float("-inf"),
                                            "else": "$rank"
                                        }
                                    }
                                },
                                "name": { "$first": "$name" }
                            }
                        }
                    ]

                result = collection.aggregate(query) 
                df = pd.DataFrame(list(result)) 
                
                styled_df = df.style.applymap(lambda x: f'font-size: 20px', subset=pd.IndexSlice[:, :])

                
                st.dataframe(styled_df, height=500,width =700)
                return df
                
            #creation des champs pour que l'utilisateur puisse rentrer les parametres qu'il souhaite dans les queries
            st.subheader('The average ranking of the movies of a director name X ')
            user_first_name = st.text_input('Enter first name:', 'Agnès')
            user_last_name = st.text_input('Enter last name:', 'Varda')
          
            #Execution des queries en fonction du bouton appuyé par l'analyst avec affichage des graphs.
            st.write("default : actor is Agnes Varda")
            if st.button('Run Query 1'):
                df =run_mongo_query_analyst('Query 1',user_first_name,user_last_name)

            st.write("----")
            st.subheader('All the directors who made movies of N different genres only.')
            n_genres = st.text_input('Enter number of genres: 2')
            st.write("default : N = 2")
            if st.button('Run Query 2'):
                df = run_mongo_query_analyst('Query 2',n_genres,None)
                count_filtered_directors = len(df)
                total_directors = 86880  
                count_other_directors = total_directors - count_filtered_directors
                labels = ['Number of directors who made movies of N different genres :' + str(len(df)), 'Other directors :' + str(count_other_directors)]
                sizes = [count_filtered_directors, count_other_directors]

                st.subheader('Number of directors who made movies of N different genres and other directors')

                fig, ax = plt.subplots()
                ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
                ax.axis('equal') 
                st.pyplot(fig)
            st.write("----")
            st.subheader('All the movies with less than X actors and with a ranking above N.')
            n_actors = st.text_input('Enter number of actors: 5')
            n_rank = st.text_input('Enter rank: 7')
            st.write("default : X = 5 and N = 7")
            if st.button('Run Query 3'):
                df = run_mongo_query_analyst('Query 3',n_actors,n_rank)
                count_filtered_movies = len(df)
                total_movies = 388269  
                count_other_movies = total_movies - count_filtered_movies
                labels = ['Movies with less than X actors with ranking above N :' +str(len(df)) , 'Other movies :' + str(count_other_movies)]
                sizes = [count_filtered_movies, count_other_movies]

                st.subheader('Number of movies meeting criteria vs other movies')

                fig, ax = plt.subplots()
                ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
                ax.axis('equal') 
                st.pyplot(fig)
            st.write("----")
            st.subheader('The movies with the best ranking by genre among the movies of the list of actors : X')
            list_actors = st.text_input('Enter list of actors : Mike Nichols, Arthur Penn etc...')
            st.write("default : X = (We will take 'Mike Nichols', 'Arthur Penn', 'William Friedkin', 'Stanley Kubrick', 'Robert Altman', 'Milos Forman', 'Martin Scorsese', 'Woody Allen', 'Michael Cimino', 'Francis Ford Coppola')")
            list_actors = [actor.strip().strip("'") for actor in list_actors.split(',')]
            if st.button('Run Query 4'):
                df =run_mongo_query_analyst('Query 4',list_actors,None)
                fig, ax = plt.subplots(figsize=(10, 6))
                df.plot(kind='bar', x='_id', y='bestRank', color='skyblue',ax = ax)
                ax.set_xlabel('Genres')
                ax.set_ylabel('Ratings')
                ax.set_title('Movie Ratings by Genre')
                ax.set_xticklabels(df['_id'], rotation=90, ha='right')
                plt.tight_layout()
                st.pyplot(fig)

            st.write("----")
if __name__ == '__main__':
    main()






