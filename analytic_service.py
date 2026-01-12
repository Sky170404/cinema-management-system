from db_utils import get_db_connection, get_mongo_db

class AnalyticService:
    @staticmethod
    def get_sql_report(emp_id):
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            query = """
                SELECT
                    m.MovieID,
                    m.Title AS MovieTitle,
                    m.AgeRating,
                    e.Name AS AddedBy,
                    COUNT(t.TrailerID) AS TrailerCount
                FROM
                    Movie m
                JOIN
                    Trailer t ON t.MovieID = m.MovieID
                JOIN
                    Employee e ON e.EmployeeID = %s
                WHERE
                    m.AgeRating IN ('12+', '16+', '18+')
                GROUP BY
                    m.MovieID, m.Title, m.AgeRating, e.Name
                ORDER BY
                    TrailerCount DESC
            """
            cursor.execute(query, (emp_id,))
            return cursor.fetchall()
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def get_nosql_report(emp_id):
        client, db = get_mongo_db()
        try:
            # Get Context name from SQL
            sql_conn = get_db_connection()
            sql_cursor = sql_conn.cursor()
            sql_cursor.execute("SELECT Name FROM Employee WHERE EmployeeID = %s", (emp_id,))
            emp_row = sql_cursor.fetchone()
            emp_name = emp_row['Name'] if emp_row else "Unknown"
            sql_cursor.close()
            sql_conn.close()

            # MongoDB Aggregation Pipeline
            pipeline = [
                {"$match": {"AgeRating": {"$in": ['12+', '16+', '18+']}}},
                {"$project": {
                    "MovieID": 1,
                    "MovieTitle": "$Title",
                    "AgeRating": 1,
                    "TrailerCount": {"$size": {"$ifNull": ["$trailers", []]}},
                    "AddedBy": {"$literal": emp_name}
                }},
                {"$match": {"TrailerCount": {"$gt": 0}}},
                {"$sort": {"TrailerCount": -1}}
            ]
            return list(db.movies.aggregate(pipeline))
        finally:
            client.close()