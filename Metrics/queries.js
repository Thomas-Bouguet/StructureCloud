// Liste des queries à lancer sur la base dénormalisée selon le schéma 1 sharding 2
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! ATTENTION !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
// Pour les exécuter sur mongodb Compass après avoir chargé les JSON, il faut convertir les type de plusieurs champs.
// Les requêtes nécessaires à cette opération sont en bas de ce fichier. 

// Ru1 (testée et validée)
db.movies.find({
    "list_genres": "Sci-Fi",
    "rank": {
        $gt: 8
    }
},{
    "name": 1,
    "rank": 1
});

// Ru2 (testée et validé)
db.directors.find({
    "dict_genres_den_prob.Romance": {
        $exists: true,
        $ne: [ null, 0 ]
    }
},{
    "first_name": 1,
    "last_name": 1
})

// Ru3 (testée et validée)
db.movies.find({
    "list_genres": "Animation",
    "year": 1995
},{
    "name": 1
});

// Ru4 (testée et validée)
const kurosawa_movies = db.directors.findOne({
    "first_name": "Akira",
    "last_name": "Kurosawa"
},{
    "list_movies_id": 1
});

const kurosawa_movies_id = kurosawa_movies ? kurosawa_movies.list_movies_id : [];

db.movies.find({
    "id": {
        $elemMatch: {
            $in: kurosawa_movies_id
        }
    },
    "list_genres": "Historical"
});


// Rda1 (testée et validée)
const director = db.directors.findOne({
    "first_name": "Agnès",
    "last_name": "Varda"
});

const list_movies_id = director ? director.list_movies_id : [];

db.movies.aggregate([{
    $match: {
        "id": { $in: list_movies_id }
    }
},{
    $group: {
        _id: null,
        averageRank: {
            $avg: "$rank"
        }
    }
},{
    $project: {
        averageRank: 1
    }
}]);

// Rda2 (testée et validée)
db.directors.aggregate([{
    $project: {
        "first_name": 1,
        "last_name": 1,
        filteredGenres: {
            $filter: {
                input: {
                    $objectToArray: "$dict_genres_den_prob"
                },
                as: "genre",
                cond: {
                    $and: [
                        { $ne: [ "$genre.v", null ] },
                        { $ne: [ "$genre.v", 0 ] }
                    ]
                }
            }
        }
    }
},{
    $match: {
        $and: [
            {
                "filteredGenres": { $exists: true }
            },
            {
                "filteredGenres": { $ne: null }
            },
            {
                $expr: {
                    $eq: [
                        { $size: "$filteredGenres" },
                        2
                    ]
                }
            }
        ]
    }
},{
    $project: {
        first_name: 1,
        last_name: 1
    }
}])

// Rda3 (testée et validée)
db.movies.find({
    "den_nb_actors": { $lt: 5 },
    "rank": { $gt: 7 }
},{
    "name": 1
})

// Rda4 () (testée et validée)
const directorsNewH = db.directors.aggregate([{
    $project: {
        "id": 1,
        "full_name": {
            $concat: [ "$first_name", " ", "$last_name" ]
        }
    }
},{
    $match: {
        "full_name": {
            $in: [
                "Mike Nichols",
                "Arthur Penn",
                "William Friedkin",
                "Stanley Kubrick",
                "Robert Altman",
                "Milos Forman",
                "Martin Scorcese",
                "Woody Allen",
                "Michael Cimino",
                "Francis Ford Coppola"
            ]
        }
    }
},{
    $unwind: "$list_movies_id"
},{
    $group: {
        _id: null,
        list_director_id: { $push: "$list_movies_id" }
    }
}])

const directorsNewH_array = directorsNewH.toArray();
const directorsNewH_id = directorsNewH_array[0].list_director_id;

db.movies.aggregate([{
    $match: {
        "id": {
            $in: directorsNewH_id
        }
    }
},{
    $unwind: "$list_genres"
},{
    $group: {
        _id: "$list_genres",
        bestRank: {
            $top: {
                output: [ "$name", "$rank" ],
                sortBy: { "rank": -1 }
            }
        }
    }
}]);



// Change fields type in mongodb Compass

// Actors id
db.actors.update(
    {},
    [{
        $set: {
            "id": {
                $convert:
                {
                    input: "$id",
                    to: "int",
                    onError: null,
                    onNull: null
                }
            }
        }
    }],
    {
        multi: true
    }
);

// Actors list_movies_id
db.actors.update(
    {},
    [
        {
            $set: {
                "list_movies_id": {
                    $map: {
                        input: "$list_movies_id",
                        as: "movieId",
                        in: { $toInt: "$$movieId" }
                    }
                }
            }
        }
    ],
    {
        multi: true
    }
);


// Directors id
db.directors.updateMany(
    {},
    {
        $set: {
            "id": {
                $toInt: "$id"
                // $convert:
                // {
                //     input: "$id",
                //     to: "int",
                //     onError: null,
                //     onNull: null
                // }
            }
        }
    },
    {
        multi: true
    }
);

// Directors list_movies_id
db.directors.updateMany(
    {},
    [{
        $set: {
            "list_movies_id": {
                $map: {
                    input: "$list_movies_id",
                    as: "movieId",
                    in: { $toInt: "$$movieId" }
                }
            }
        }
    }]
);

// Directors dict_genres_den_prob
db.directors.update(
    {},
    [
        {
            $set: {
                "dict_genres_den_prob": {
                    $arrayToObject: {
                        $map: {
                            input: { $objectToArray: "$dict_genres_den_prob" },
                            as: "pair",
                            in: {
                                k: "$$pair.k",
                                v: { $toDouble: "$$pair.v" }
                            }
                        }
                    }
                }
            }
        }
    ],
    {
        multi: true
    }
);

// Movies id
db.movies.update(
    {},
    [{
        $set: {
            "id": {
                $convert:
                {
                    input: "$id",
                    to: "int",
                    onError: null,
                    onNull: null
                }
            }
        }
    }],
    {
        multi: true
    }
);

// Movies rank
db.movies.update(
    {},
    [{
        $set: {
            "rank": {
                $convert:
                {
                    input: "$rank",
                    to: "double",
                    onError: null,
                    onNull: null
                }
            }
        }
    }],
    {
        multi: true
    }
);

// Movies list_directors_id
db.movies.update(
    {},
    [
        {
            $set: {
                "list_directors_id": {
                    $map: {
                        input: "$list_directors_id",
                        as: "directorId",
                        in: { $toInt: "$$directorId" }
                    }
                }
            }
        }
    ],
    {
        multi: true
    }
);
