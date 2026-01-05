from flask import Flask, render_template, jsonify
import pymysql
from faker import Faker
import random
from datetime import datetime, timedelta

app = Flask(__name__)

DB_HOST = 'db'
DB_USER = 'root'
DB_PASSWORD = 'root'
DB_NAME = 'cinema'

fake = Faker()

def get_db_connection():
    return pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, db=DB_NAME, cursorclass=pymysql.cursors.DictCursor)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/import', methods=['POST'])
def import_data():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        delete_order = ['Ticket', 'Trailer', 'handles', 'employs', 'Screening', 'Customer', 'Movie', 'Room', 'Manager', 'Worker', 'Employee']
        for table in delete_order:
            cursor.execute(f"DELETE FROM {table}")

        for i in range(1, 11):
            cursor.execute("""
                INSERT INTO Employee (EmployeeID, Name, Salary, Email)
                VALUES (%s, %s, %s, %s)
            """, (i, fake.name(), round(random.uniform(2000, 5000), 2), fake.email()))

        for i in range(1, 6):
            cursor.execute("""
                INSERT INTO Manager (EmployeeID, LeadershipExperience, Department)
                VALUES (%s, %s, %s)
            """, (i, f"{random.randint(3,15)} years", random.choice(['Operations', 'HR', 'Technical'])))

        for i in range(6, 11):
            cursor.execute("""
                INSERT INTO Worker (EmployeeID, Position, WorkingHours)
                VALUES (%s, %s, %s)
            """, (i, random.choice(['Cleaner', 'Technician', 'Usher']), random.randint(20,40)))

        for i in range(1, 11):
            cursor.execute("""
                INSERT INTO Movie (MovieID, Title, Description, AgeRating, ThumbnailURL, ReleaseYear)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (i, fake.sentence(nb_words=5)[:-1], fake.paragraph(), random.choice(['0+', '6+', '12+', '16+', '18+']), fake.image_url(), random.randint(2015,2025)))

        for i in range(1, 6):
            cursor.execute("""
                INSERT INTO Room (RoomID, Capacity, ScreeningType)
                VALUES (%s, %s, %s)
            """, (i, random.randint(80,250), random.choice(['2D', '3D', 'IMAX', '4D'])))

        screening_id = 1
        for movie_id in range(1, 11):
            for _ in range(random.randint(3,8)):
                room_id = random.randint(1,5)
                showtime = datetime.now() + timedelta(days=random.randint(-10,15), hours=random.randint(10,23))
                available_seats = random.randint(20, 250)
                cursor.execute("""
                    INSERT INTO Screening (ScreeningID, MovieID, RoomID, Showtime, AvailableSeats)
                    VALUES (%s, %s, %s, %s, %s)
                """, (screening_id, movie_id, room_id, showtime, available_seats))
                screening_id += 1

        for movie_id in range(1, 11):
            num_trailers = random.randint(1,4)
            for trailer_id in range(1, num_trailers + 1):
                cursor.execute("""
                    INSERT INTO Trailer (MovieID, TrailerID, URL, Description)
                    VALUES (%s, %s, %s, %s)
                """, (movie_id, trailer_id, fake.url(), fake.sentence()))

        for i in range(1, 16):
            payment = random.choice(['creditcard', 'PayPal', 'cash'])
            cursor.execute("""
                INSERT INTO Customer (CustomerID, Name, BirthDate, PaymentReference)
                VALUES (%s, %s, %s, %s)
            """, (i, fake.name(), fake.date_of_birth(minimum_age=16, maximum_age=90), payment))

        ticket_id = 1
        cursor.execute("SELECT ScreeningID, AvailableSeats FROM Screening")
        screenings = cursor.fetchall()
        for screening in screenings:
            screening_id = screening['ScreeningID']
            max_seats = screening['AvailableSeats'] or 100 
            num_tickets = random.randint(0, min(40, max_seats))
            used_seats = set()
            for _ in range(num_tickets):
                while True:
                    seat = random.randint(1, max_seats)
                    if seat not in used_seats:
                        used_seats.add(seat)
                        break
                price = round(random.uniform(8.5, 20.0), 2)
                customer_id = random.randint(1, 15)
                cursor.execute("""
                    INSERT INTO Ticket (TicketID, ScreeningID, Price, Seat, CustomerID)
                    VALUES (%s, %s, %s, %s, %s)
                """, (ticket_id, screening_id, price, seat, customer_id))
                ticket_id += 1

        for worker_id in range(6,11):
            rooms = random.sample(range(1,6), random.randint(1,4))
            for room_id in rooms:
                cursor.execute("""
                    INSERT IGNORE INTO handles (WorkerID, RoomID)
                    VALUES (%s, %s)
                """, (worker_id, room_id))

        conn.commit()
        return jsonify({"message": "Data imported successfully! All tables cleared and filled with new random data."})

    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@app.route('/status')
def status():
    return '''
    <!DOCTYPE html>
    <html lang="de">
    <head>
        <meta charset="UTF-8">
        <title>Database-state</title>
        <style>
            body { background-color: #141414; color: #fff; font-family: 'Helvetica Neue', Arial, sans-serif; margin: 0; padding: 20px; }
            h1 { color: #e50914; text-align: center; }
            .container { max-width: 1200px; margin: 0 auto; }
            .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 20px; margin-top: 40px; }
            .card { background: #221f1f; padding: 20px; border-radius: 8px; text-align: center; }
            .card h3 { color: #e50914; margin: 0 0 15px 0; }
            .btn { background: #e50914; color: white; padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer; font-size: 16px; }
            .btn:hover { background: #f40612; }
            a { text-decoration: none; }
            .back { text-align: center; margin: 40px 0; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Datenbank-Status</h1>
            <p style="text-align:center;">Klick on an entity to reveal its data</p>
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
            <div class="back"><a href="/"><button class="btn">← Back to start</button></a></div>
        </div>
    </body>
    </html>
    '''


@app.route('/table/<table_name>')
def show_table(table_name):
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
                <a href="/status"><button class="btn">← Back to overview</button></a>
                <a href="/"><button class="btn">← back to main page</button></a>
            </div>
        </body></html>
        '''
    except Exception as e:
        return f"<h2 style='color:#f00;'>Error: {str(e)}</h2><a href='/status'>Back</a>"
    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)