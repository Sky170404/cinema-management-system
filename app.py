from analytic_service import AnalyticService
from db_utils import get_db_connection, get_mongo_db
from data_service import run_data_generation, run_mongo_migration
from flask import Flask, render_template, jsonify, request,redirect
from faker import Faker
import random
from datetime import datetime, timedelta, date
from employee_service import EmployeeService
from usecase_service import UsecaseService

app = Flask(__name__)
fake = Faker()
@app.route('/')
def index():
    conn = get_db_connection()
    cursor = conn.cursor()
    employees = []
    try:
        query = """
                SELECT 
                        e.EmployeeID, 
                        e.Name, 
                        CASE 
                            WHEN m.EmployeeID IS NOT NULL THEN 'Manager'
                            WHEN w.EmployeeID IS NOT NULL THEN w.Position
                            ELSE 'Staff'
                        END as RoleInfo
                    FROM Employee e
                    LEFT JOIN Manager m ON e.EmployeeID = m.EmployeeID
                    LEFT JOIN Worker w ON e.EmployeeID = w.EmployeeID
                    ORDER BY e.Name
                """
        cursor.execute(query)
        employees = cursor.fetchall()
    except Exception as e:
        print(f"Index query failed (possibly empty DB): {e}")
    finally:
        cursor.close()
        conn.close()
    return render_template('index.html', employees=employees)

@app.route('/generate', methods=['POST'])
def generate_data():
    try:
        result = run_data_generation()
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/status')
def status():
    emp_id = request.args.get('employee_id', '')
    return f'''
       <!DOCTYPE html>
       <html lang="de">
       <head>
           <meta charset="UTF-8">
           <title>SQL Database Tables</title>
           <style>
               body {{ background-color: #141414; color: #fff; font-family: 'Helvetica Neue', Arial, sans-serif; margin: 0; padding: 20px; }}
               h1 {{ color: #e50914; text-align: center; }}
               .container {{ max-width: 1200px; margin: 0 auto; }}
               .grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 20px; margin-top: 40px; }}
               .card {{ background: #221f1f; padding: 20px; border-radius: 8px; text-align: center; }}
               .card h3 {{ color: #e50914; margin: 0 0 15px 0; }}
               .btn {{ background: #e50914; color: white; padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer; font-size: 16px; }}
               .btn:hover {{ background: #f40612; }}
               .back-btn {{ background: #333; color: white; padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer; margin-top: 20px; }}
               a {{ text-decoration: none; }}
               .back {{ text-align: center; margin: 40px 0; }}
           </style>
       </head>
       <body>
           <div class="container">
            <h1>SQL Database Tables</h1>
            <p style="text-align:center;">Click on an entity to reveal its data</p>
            <div class="grid">
                <div class="card"><h3>Employee</h3><a href="/table/Employee"><button class="btn">Show</button></a></div>
                <div class="card"><h3>Manager</h3><a href="/table/Manager"><button class="btn">Show</button></a></div>
                <div class="card"><h3>Worker</h3><a href="/table/Worker"><button class="btn">Show</button></a></div>
                <div class="card"><h3>Movie</h3><a href="/table/Movie"><button class="btn">Show</button></a></div>
                <div class="card"><h3>Trailer</h3><a href="/table/Trailer"><button class="btn">Show</button></a></div>
                <div class="card"><h3>Room</h3><a href="/table/Room"><button class="btn">Show</button></a></div>
                <div class="card"><h3>Screening</h3><a href="/table/Screening"><button class="btn">Show</button></a></div>
                <div class="card"><h3>Customer</h3><a href="/table/Customer"><button class="btn">Show</button></a></div>
                <div class="card"><h3>Ticket</h3><a href="/table/Ticket"><button class="btn">Show</button></a></div>
                <div class="card"><h3>handles</h3><a href="/table/handles"><button class="btn">Show</button></a></div>
            </div>
            <div class="back"><button class="back-btn" onclick="window.location.href='/?employee_id={emp_id}'">Back to Home</button></div>
        </div>
    </body>
    </html>
    '''


@app.route('/table/<table_name>')
def show_table(table_name):
    emp_id = request.args.get('employee_id', '')
    allowed_tables = ['Employee', 'Manager', 'Worker', 'Movie', 'Trailer', 'Room', 'Screening', 'Customer', 'Ticket', 'handles']
    if table_name not in allowed_tables:
        return "<h2 style='color:#e50914;'>invalid table!</h2><a href='/status'>Back</a>", 400

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT COUNT(*) AS cnt FROM {table_name}")
        count = cursor.fetchone()['cnt']

        cursor.execute(f"SELECT * FROM {table_name} LIMIT 100")
        rows = cursor.fetchall()

        if rows:
            columns = rows[0].keys()
            table_html = "<table style='width:100%; border-collapse: collapse; margin:20px 0;'>"
            table_html += "<tr style='background:#e50914; color:white;'>"
            for col in columns:
                table_html += f"<th style='padding:10px; border:1px solid #333;'>{col}</th>"
            table_html += "</tr>"
            for row in rows:
                table_html += "<tr style='background:#221f1f;'>"
                for col in columns:
                    value = row[col] if row[col] is not None else "NULL"
                    table_html += f"<td style='padding:8px; border:1px solid #333;'>{value}</td>"
                table_html += "</tr>"
            table_html += "</table>"
        else:
            table_html = "<p>No data is this table.</p>"

        return f'''
        <!DOCTYPE html>
        <html><head><meta charset="UTF-8"><title>{table_name}</title>
        <style>
            body {{ background:#141414; color:#fff; font-family:Arial; padding:20px; }}
            h1 {{ color:#e50914; text-align:center; }}
            .back {{ text-align:center; margin:30px; }}
            .btn {{ background:#e50914; color:white; padding:12px 24px; border:none; border-radius:4px; cursor:pointer; }}
        </style></head>
        <body>
            <h1>{table_name} – {count} Entries</h1>
            {table_html}
            <div class="back">
                <a href="/status?employee_id={emp_id}"><button class="btn">← Back to overview</button></a>
                <button class="back-btn" onclick="window.location.href='/?employee_id={emp_id}'">Back to Home</button>
            </div>
        </body></html>
        '''
    except Exception as e:
        return f"<h2 style='color:#f00;'>Error: {str(e)}</h2><a href='/status'>Back</a>"
    finally:
        cursor.close()
        conn.close()


# --- CLEANING MANAGEMENT (USE CASE & ANALYTICS) ---
@app.route('/cleaning-michelle', methods=['GET', 'POST'])
def cleaning_assignment_michelle():
    emp_id = request.args.get('employee_id')
    
    if not emp_id or not EmployeeService.is_manager(emp_id):
        return """
        <h1 style="color:#e50914; text-align:center;">Access Denied</h1>
        <p style="text-align:center;">This page is restricted to Managers only.</p>
        <div style="text-align:center; margin-top:30px;">
            <a href="/" style="color:#e50914; text-decoration:none;">
                <button class="btn" style="padding:12px 24px;">← Back to Home</button>
            </a>
        </div>
        """, 403

    conn = None
    cursor = None
    message = None
    manager_name = "Manager"

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT Name FROM Employee WHERE EmployeeID = %s", (emp_id,))
        manager_row = cursor.fetchone()
        if manager_row:
            manager_name = manager_row['Name']

        if request.method == 'POST':
            if 'delete' in request.form:
                worker_id = request.form['delete_worker']
                room_id = request.form['delete_room']
                cursor.execute("DELETE FROM handles WHERE WorkerID = %s AND roomID = %s", (worker_id, room_id))
                conn.commit()
                message = f"Zuweisung Worker {worker_id} → Room {room_id} entfernt!"
            else:
                room_id = request.form['room']
                worker_ids = request.form.getlist('workers')
                assigned_count = 0
                for worker_id in worker_ids:
                    cursor.execute("INSERT IGNORE INTO handles (WorkerID, RoomID) VALUES (%s, %s)", (worker_id, room_id))
                    assigned_count += 1
                conn.commit()
                message = f"{assigned_count} Worker zu Room {room_id} zugewiesen!"

        position_filter = request.args.get('position', '')

        cursor.execute("""
            SELECT DISTINCT r.RoomID, r.Capacity, r.ScreeningType
            FROM Room r
            JOIN Screening s ON r.RoomID = s.RoomID
            WHERE s.Showtime < NOW()
            ORDER BY r.RoomID
        """)
        rooms = cursor.fetchall() or []

        position_query = ""
        if position_filter in ['Cleaner', 'Technician', 'Usher']:
            position_query = f" AND w.Position = '{position_filter}'"
        cursor.execute("""
            SELECT w.EmployeeID, e.Name, w.Position
            FROM Worker w
            JOIN Employee e ON w.EmployeeID = e.EmployeeID
            WHERE w.Position IN ('Cleaner', 'Technician', 'Usher') """ + position_query + """
            ORDER BY e.Name
        """)
        workers = cursor.fetchall() or []

        cursor.execute("""
            SELECT e.Name, w.Position, w.EmployeeID,
                   GROUP_CONCAT(DISTINCT h.RoomID) AS assigned_rooms_list,
                   COUNT(DISTINCT h.RoomID) AS assigned_count,
                   w.WorkingHours
            FROM Worker w
            JOIN Employee e ON w.EmployeeID = e.EmployeeID
            LEFT JOIN handles h ON w.EmployeeID = h.WorkerID
            GROUP BY w.EmployeeID
            HAVING assigned_count > 0 
            ORDER BY assigned_count DESC
        """)
        workload = cursor.fetchall() or []

        return render_template('cleaning.html', 
                               rooms=rooms, 
                               workers=workers, 
                               workload=workload, 
                               message=message, 
                               position_filter=position_filter,
                               manager_name=manager_name)

    except Exception as e:
        print(f"Cleaning error: {str(e)}")
        return f"<h1>Error: {str(e)}</h1><p>Check Docker logs!</p>", 500

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


#check if Manager is logged in
@app.route('/check-cleaning-access')
def check_cleaning_access():
    emp_id = request.args.get('employee_id')
    
    if not EmployeeService.is_manager(emp_id):
        return jsonify({"access": False, "message": "Only Managers can access Cleaning Assignment"})
    
    return jsonify({"access": True, "redirect": f"/cleaning-michelle?employee_id={emp_id}"})


# --- NOSQL USE CASE: MONGODB CLEANING MANAGEMENT ---
@app.route('/cleaning-michelle-mongo', methods=['GET', 'POST'])
def cleaning_assignment_mongo():
    emp_id = request.args.get('employee_id')

    if not emp_id or not EmployeeService.is_manager(emp_id):
        return """
        <h1 style="color:#e50914; text-align:center;">Access Denied</h1>
        <p style="text-align:center;">This page is restricted to Managers only.</p>
        <p style="text-align:center;">Please select a Manager from the dropdown on the home page.</p>
        <div style="text-align:center; margin-top:30px;">
            <a href="/" style="color:#e50914; text-decoration:none;">
                <button class="btn" style="padding:12px 24px;">← Back to Home</button>
            </a>
        </div>
        """, 403

    mongo_client, db = get_mongo_db()
    message = None
    manager_name = "Manager"

    page_title = "NoSQL Cleaning Assigment (MongoDB)"
    data_source_info = "Data is being read and written directly from the 'employees', 'assignments', 'rooms' and 'screenings' collections."

    try:
        # get name of the manager
        manager_doc = db.employees.find_one({"employeeID": int(emp_id)})
        manager_name = manager_doc.get('name', 'Manager') if manager_doc else "Manager (ID not found)"

        if request.method == 'POST':
            if 'delete' in request.form:
                worker_id = int(request.form['delete_worker'])
                room_id = int(request.form['delete_room'])
                result = db.assignments.delete_one({"workerID": worker_id, "roomID": room_id})
                message = "Assignment removed!" if result.deleted_count > 0 else "No Assigment found."
            else:
                room_id = int(request.form['room'])
                worker_ids = [int(w) for w in request.form.getlist('workers')]
                assigned_count = 0
                for worker_id in worker_ids:
                    if not db.assignments.find_one({"workerID": worker_id, "roomID": room_id}):
                        db.assignments.insert_one({
                            "workerID": worker_id,
                            "roomID": room_id,
                            "assignedAt": datetime.utcnow()
                        })
                        assigned_count += 1
                message = f"{assigned_count} Worker to room {room_id} assigned!"

        position_filter = request.args.get('position', '')

        #find rooms which are finished with screenings
        finished_room_ids = db.screenings.distinct(
            "RoomID",
            {"Showtime": {"$lt": datetime.utcnow()}}
        )

        rooms = list(
            db.rooms.find(
                {"RoomID": {"$in": finished_room_ids}},
                {
                    "_id": 0,
                    "RoomID": 1,
                    "Capacity": 1,
                    "ScreeningType": 1
                }
            ).sort("RoomID", 1)
        )

        # Workers
        workers_query = {"role": "Worker", "position": {"$in": ["Cleaner", "Technician", "Usher"]}}
        if position_filter:
            workers_query["position"] = position_filter
        workers = list(db.employees.find(workers_query, {"employeeID": 1, "name": 1, "position": 1}).sort("name", 1))

        workload = list(db.assignments.aggregate([
            {"$group": {
                "_id": "$workerID",
                "assigned_rooms": {"$addToSet": "$roomID"},
                "assigned_count": {"$sum": 1}
            }},
            {"$match": {"assigned_count": {"$gt": 0}}},
            {"$lookup": {
                "from": "employees",
                "localField": "_id",
                "foreignField": "employeeID",
                "as": "worker"
            }},
            {"$unwind": "$worker"},
            {"$match": {
                "worker.position": {"$in": ["Cleaner", "Technician", "Usher"]}
            }},
            {"$project": {
                "Name": "$worker.name",
                "Position": "$worker.position",
                "EmployeeID": "$_id",
                "assigned_rooms_list": {
                    "$reduce": {
                        "input": "$assigned_rooms",
                        "initialValue": "",
                        "in": {
                            "$concat": [
                                "$$value",
                                {"$cond": [{"$eq": ["$$value", ""]}, "", ", "]},
                                {"$toString": "$$this"}
                            ]
                        }
                    }
                },
                "assigned_count": "$assigned_count",
                "WorkingHours": "$worker.workingHours"
            }},
            {"$sort": {"assigned_count": -1}}
        ]))

        return render_template('cleaning_mongo.html', 
                               rooms=rooms, 
                               workers=workers, 
                               workload=workload, 
                               message=message, 
                               position_filter=position_filter,
                               employee_id=emp_id,
                               manager_name=manager_name,
                               page_title=page_title,
                               data_source_info=data_source_info)

    except Exception as e:
        print(f"NoSQL Cleaning error: {str(e)}")
        return f"<h1>Error: {str(e)}</h1><p>Check Docker logs!</p>", 500
    finally:
        mongo_client.close()


# --- NOSQL SETUP ---
@app.route('/migrate-to-mongo', methods=['POST'])
def migrate_to_mongo():
    try:
        result = run_mongo_migration()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/mongo-status')
def mongo_status():
    emp_id = request.args.get('employee_id')
    try:
        mongo_client, db = get_mongo_db()

        counts = {
            "employees": db.employees.count_documents({}),
            "movies": db.movies.count_documents({}),
            "rooms": db.rooms.count_documents({}),
            "screenings": db.screenings.count_documents({}),
            "customers": db.customers.count_documents({}),
            "tickets": db.tickets.count_documents({}),
            "assignments": db.assignments.count_documents({})
        }

        mongo_client.close()

        return render_template('mongo_status.html', counts=counts, employee_id=emp_id)

    except Exception as e:
        return f"<h1>Error: {str(e)}</h1>"


@app.route('/mongo-collection/<collection_name>')
def mongo_collection(collection_name):
    emp_id = request.args.get('employee_id')
    try:
        mongo_client, db = get_mongo_db()

        if collection_name not in ['employees', 'movies', 'rooms', 'screenings', 'customers', 'tickets', 'assignments']:
            return "<h2>Invalid collection!</h2><a href='/mongo-status'>Back</a>", 400

        docs = list(db[collection_name].find().limit(1000))

        return render_template('mongo_collection.html', 
                               collection_name=collection_name, 
                               docs=docs, employee_id=emp_id)

    except Exception as e:
        return f"<h1>Error: {str(e)}</h1>"
    finally:
        mongo_client.close()


# --- MOVIE & TRAILER MANAGEMENT (USE CASE & ANALYTICS) ---
@app.route('/movie-management')
def movie_management():
    conn = get_db_connection()
    cursor = conn.cursor()
    message = request.args.get('message')
    emp_id = request.args.get('employee_id')

    if not EmployeeService.is_marketing_staff(emp_id):
        return "<h1>Access Denied</h1><p>Restricted to Marketing team.</p><a href='/'>Back Home</a>", 403

    try:
        # Query combines Movie with Trailer counts
        query = """
                SELECT m.MovieID, m.Title, m.AgeRating, COUNT(t.TrailerID) as TrailerCount
                FROM Movie m
                         LEFT JOIN Trailer t ON m.MovieID = t.MovieID
                GROUP BY m.MovieID, m.Title, m.AgeRating
                ORDER BY m.Title ASC \
                """
        cursor.execute(query)
        movies = cursor.fetchall()
        return render_template('movie_management.html', movies=movies, message=message, employee_id=emp_id)
    finally:
        cursor.close()
        conn.close()
@app.route('/add-trailer-form/<int:movie_id>')
def add_trailer_form(movie_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT MovieID, Title FROM Movie WHERE MovieID = %s", (movie_id,))
        movie = cursor.fetchone()
        return render_template('add_trailer.html', movie=movie)
    finally:
        cursor.close()
        conn.close()
@app.route('/save-trailer', methods=['POST'])
def save_trailer():
    movie_id = request.form.get('movie_id')
    url = request.form.get('url')
    description = request.form.get('description')
    emp_id = request.form.get('employee_id')
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Ensure URL uniqueness for this movie
        cursor.execute("SELECT * FROM Trailer WHERE MovieID = %s AND URL = %s", (movie_id, url))
        if cursor.fetchone():
            return f"<h1>Error</h1><p>This trailer URL already exists for this movie.</p><a href='/add-trailer-form/{movie_id}'>Try again</a>"
        # Generate unique TrailerID (Max+1)
        cursor.execute("SELECT COALESCE(MAX(TrailerID), 0) + 1 as next_id FROM Trailer WHERE MovieID = %s",
                       (movie_id,))
        next_id = cursor.fetchone()['next_id']
        # Create record
        cursor.execute("""
                       INSERT INTO Trailer (MovieID, TrailerID, URL, Description)
                       VALUES (%s, %s, %s, %s)
                       """, (movie_id, next_id, url, description))
        conn.commit()
        return redirect(f'/movie-management?message=Trailer+Added+Successfully&employee_id={emp_id}')
    except Exception as e:
        conn.rollback()
        return f"<h1>Database Error</h1><p>{str(e)}</p>"
    finally:
        cursor.close()
        conn.close()

# --- NOSQL USE CASE: MONGODB TRAILER MANAGEMENT ---
@app.route('/movie-management-mongo')
def movie_management_mongo():
    emp_id = request.args.get('employee_id')

    if not EmployeeService.is_marketing_staff(emp_id):
        return "<h1>Access Denied</h1><p>Restricted to Marketing team.</p><a href='/'>Back Home</a>", 403

    try:
        mongo_client, db = get_mongo_db()

        movies = list(db.movies.find({}, {"MovieID": 1, "Title": 1, "AgeRating": 1, "trailers": 1}))
        mongo_client.close()
        return render_template('movie_management_mongo.html', movies=movies, employee_id=emp_id)
    except Exception as e:
        return f"<h1>Mongo Error</h1><p>{str(e)}</p>"
@app.route('/add-trailer-form-mongo/<int:movie_id>')
def add_trailer_form_mongo(movie_id):
    try:
        mongo_client, db = get_mongo_db()

        movie = db.movies.find_one({"MovieID": movie_id})
        mongo_client.close()
        return render_template('add_trailer_mongo.html', movie=movie)
    except Exception as e:
        return f"<h1>Error</h1><p>{str(e)}</p>"


@app.route('/save-trailer-mongo', methods=['POST'])
def save_trailer_mongo():
    movie_id = int(request.form.get('movie_id'))
    url = request.form.get('url')
    description = request.form.get('description')
    emp_id = request.form.get('employee_id')
    try:
        mongo_client, db = get_mongo_db()

        existing = db.movies.find_one({"MovieID": movie_id, "trailers.URL": url})
        if existing:
            mongo_client.close()
            return f"<h1>NoSQL Error</h1><p>This trailer URL already exists in MongoDB.</p><a href='/movie-management-mongo'>Back</a>"

        new_trailer = {
            "TrailerID": random.randint(10000, 99999),
            "URL": url,
            "Description": description
        }

        db.movies.update_one(
            {"MovieID": movie_id},
            {"$push": {"trailers": new_trailer}}
        )

        mongo_client.close()
        return redirect(f'/movie-management-mongo?message=Trailer+Added+to+NoSQL&employee_id={emp_id}')
    except Exception as e:
        return f"<h1>MongoDB Save Error</h1><p>{str(e)}</p>"
    finally:
        if 'mongo_client' in locals():
            mongo_client.close()
# --- END NOSQL ROUTES ---
# --- END MANAGEMENT ROUTES ---




# --- PROMOTIONAL ANALYTICS ROUTES ---
@app.route('/promotional-analytics-sql')
def analytics_sql():
    emp_id = request.args.get('employee_id', 1)
    rating = request.args.get('rating')
    if not EmployeeService.is_marketing_staff(emp_id):
        return "<h1>Access Denied</h1><p>Restricted to Marketing team.</p><a href='/'>Back Home</a>", 403

    try:
        results = AnalyticService.get_sql_report(emp_id, rating)
        return render_template('analytics.html', results=results, type='SQL', employee_id=emp_id, current_rating=rating)
    except Exception as e:
        return f"<h1>SQL Analytics Error</h1><p>{str(e)}</p>", 500

@app.route('/promotional-analytics-nosql')
def analytics_nosql():
    emp_id = request.args.get('employee_id', 1)
    rating = request.args.get('rating')
    if not EmployeeService.is_marketing_staff(emp_id):
        return "<h1>Access Denied</h1><p>Restricted to Marketing team.</p><a href='/'>Back Home</a>", 403

    try:
        results = AnalyticService.get_nosql_report(emp_id, rating)
        return render_template('analytics.html', results=results, type='NoSQL', employee_id=emp_id, current_rating=rating)
    except Exception as e:
        return f"<h1>NoSQL Analytics Error</h1><p>{str(e)}</p>", 500


@app.route('/run-use-case', methods=['POST'])
def run_use_case():
    emp_id = request.args.get('employee_id')
    db_type = request.args.get('type')  # SQL or NoSQL
    rating = request.args.get('rating', '').strip()

    #Ensure empty strings from the frontend are treated as None
    if not rating:
        rating = None

    if not EmployeeService.is_marketing_staff(emp_id):
        return jsonify({"error": "Restricted to Marketing team."}), 403

    try:
        result = UsecaseService.run_marketing_use_case(db_type, rating)
        if "error" in result:
            return jsonify(result), 400
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)