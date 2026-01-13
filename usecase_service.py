import random
from faker import Faker
from db_utils import get_db_connection, get_mongo_db

fake = Faker()


class UsecaseService:
    @staticmethod
    def run_marketing_use_case(db_type, rating_filter=None):
        if db_type == 'SQL':
            conn = get_db_connection()
            cursor = conn.cursor()
            try:
                query = "SELECT MovieID, Title FROM Movie "
                params = []

                if rating_filter:
                    query += " WHERE AgeRating = %s "
                    params.append(rating_filter)

                query += " ORDER BY RAND() LIMIT 1"

                cursor.execute(query, tuple(params))
                movie = cursor.fetchone()

                if not movie:
                    filter_text = f" with rating '{rating_filter}'" if rating_filter else ""
                    return {"error": f"No movies found in SQL database{filter_text}"}

                movie_id = movie['MovieID']
                movie_title = movie['Title']
                trailer_url = f"https://youtu.be/{fake.lexify('???????????')}"

                cursor.execute("""
                               SELECT COALESCE(MAX(TrailerID), 0) + 1 as next_id
                               FROM Trailer
                               WHERE MovieID = %s
                               """, (movie_id,))
                next_id = cursor.fetchone()['next_id']

                cursor.execute("""
                               INSERT INTO Trailer (MovieID, TrailerID, URL, Description)
                               VALUES (%s, %s, %s, %s)
                               """, (movie_id, next_id, trailer_url, "Automatic Use-Case Trailer"))

                conn.commit()
                return {"message": f"Successfully added SQL trailer to '{movie_title}'!"}
            finally:
                cursor.close()
                conn.close()

        else:
            mongo_client, db = get_mongo_db()
            try:
                match_stage = {"AgeRating": rating_filter} if rating_filter else {}
                pipeline = [
                    {"$match": match_stage},
                    {"$sample": {"size": 1}}
                ]
                results = list(db.movies.aggregate(pipeline))
                if not results:
                    return {"error": "No movies found in MongoDB"}

                movie = results[0]
                movie_id = movie['MovieID']
                movie_title = movie['Title']

                new_trailer = {
                    "TrailerID": random.randint(10000, 99999),
                    "URL": f"https://youtu.be/{fake.lexify('???????????')}",
                    "Description": "Automatic NoSQL Use-Case Trailer"
                }

                db.movies.update_one(
                    {"MovieID": movie_id},
                    {"$push": {"trailers": new_trailer}}
                )
                return {"message": f"Successfully added NoSQL trailer to '{movie_title}'!"}
            finally:
                mongo_client.close()