from pymongo import MongoClient
from flask import Flask, request
from bson.json_util import dumps
from flask_cors import CORS
from datetime import datetime

app = Flask(__name__)
CORS(app)
client = MongoClient('localhost:27017')
db = client.movie


@app.route('/movies', methods=["GET"])
def get_movies():
    response = db.movies.find().limit(2)
    return dumps(response)


@app.route('/moviesbyid', methods=["GET"])
def get_movie_by_id():
    response = db.movies.find_one({"id": int(request.args['movie'])})
    return dumps(response)


@app.route('/moviesbyyear', methods=['GET'])
def get_movies_by_year():
    query = {"release_date": {"$regex": request.args['year']}}
    response = {}
    response['movies'] = db.movies.find(
        query).sort("revenue", -1).limit(10)
    return dumps(response)


@app.route('/movieCountByYear', methods=['GET'])
def get_moviecount_by_year():
    query = [{"$project":
              {"result":
               {"$arrayElemAt": [{"$split": ["$release_date", "-"]}, 0]}}},
             {"$group": {"_id": "$result", "count": {"$sum": 1}}},
             {'$match': {'_id': {'$lte': str(datetime.today().year)}}},
             {"$sort": {"_id": -1}}, {"$limit": 20}, {"$sort": {"_id": 1}}]
    response = {}
    response['result'] = db.movies.aggregate(query)
    return dumps(response)


@app.route('/listGenres', methods=['GET'])
def list_all_genres():
    query = [
        {"$project": {'_id': 0, 'genres': 1}},
        {"$unwind": '$genres'},
        {"$group": {'_id': '$genres'}},
        {"$sort": {'_id.name': 1}}
    ]
    response = {}
    response['results'] = db.movies.aggregate(query)
    return dumps(response)


@app.route('/moviesinagenre', methods=['GET'])
def get_movies_in_genre():
    query = [
        {"$project": {"_id": 0, "genres": 1, "popularity": 1, "year": {
            "$arrayElemAt": [{"$split": ["$release_date", "-"]}, 0]}}},
        {"$group": {"_id": {"year": "$year"},  "movieTotal": {
            "$sum": 1}, "genres": {"$addToSet": '$genres'}}},
        {"$project":
         {"genres":
          {"$reduce":
           {"input": "$genres", "initialValue": [], "in":
            {"$concatArrays": ["$$value", "$$this"]}}}}},
        {"$unwind": '$genres'},
        {"$group": {"_id": {'genres': '$genres', 'year': '$_id.year'},
                    "genreMovies": {"$sum": 1}}},
        {'$match': {"_id.genres.id": int(request.args['genre'])}},
        {'$match': {'_id.year': {'$lte': str(datetime.today().year)}}},
        {"$sort": {"_id": -1}}, {"$limit": 20}, {"$sort": {"_id": 1}}
    ]
    response = {}
    response['results'] = db.movies.aggregate(query)
    return dumps(response)


@app.route('/popularitybygere', methods=["GET"])
def get_popularity_by_genre():
    query = [
        {"$project": {'_id': 0, 'genres': 1, 'popularity': 1, 'vote_average': 1, "year": {
            "$arrayElemAt": [{"$split": ["$release_date", "-"]}, 0]}}},
        {"$unwind": '$genres'},
        {"$group":
         {'_id':
          {'year': '$year', 'genres': '$genres'},
          "popularity": {"$avg": '$popularity'},
          "vote_average": {"$avg": '$vote_average'}}},
        {"$match": {'_id.genres.id': int(request.args['genre'])}},
        {'$match': {'_id.year': {'$lte': str(datetime.today().year)}}},
        {"$sort": {"_id": -1}}, {"$limit": 20}, {"$sort": {"_id": 1}}
    ]
    response = {}
    response['results'] = db.movies.aggregate(query)
    return dumps(response)


if __name__ == '__main__':
    app.debug = True
    app.run()
