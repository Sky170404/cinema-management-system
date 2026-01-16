from db_utils import get_db_connection, get_mongo_db

class AnalyticService:
    @staticmethod
    def get_sql_report(emp_id, rating_filter=None):
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            where_clause = ""
            params = []
            if rating_filter:
                where_clause = " AND m.AgeRating = %s "
                params.append(rating_filter)

            query = f"""
                SELECT
                    m.MovieID,
                    m.Title AS MovieTitle,
                    m.AgeRating,
                    COUNT(t.TrailerID) AS TrailerCount
                FROM
                    Movie m
                JOIN
                    Trailer t ON t.MovieID = m.MovieID
                WHERE 1=1 {where_clause}
                GROUP BY
                    m.MovieID, m.Title, m.AgeRating
                ORDER BY
                    TrailerCount DESC
            """
            cursor.execute(query, tuple(params))
            return cursor.fetchall()
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def get_nosql_report(emp_id, rating_filter=None):
        client, db = get_mongo_db()
        try:
            pipeline = []
            if rating_filter:
                pipeline.append({"$match": {"AgeRating": rating_filter}})

            pipeline.extend([
                {"$project": {
                    "MovieID": 1,
                    "MovieTitle": "$Title",
                    "AgeRating": 1,
                    "TrailerCount": {"$size": {"$ifNull": ["$trailers", []]}},
                }},
                {"$match": {"TrailerCount": {"$gt": 0}}},
                {"$sort": {"TrailerCount": -1}}
            ])
            return list(db.movies.aggregate(pipeline))
        finally:
            client.close()