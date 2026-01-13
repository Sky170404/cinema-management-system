from flask import jsonify
from db_utils import get_db_connection, get_mongo_db
from faker import Faker
import random
from datetime import datetime, timedelta, date

fake = Faker()

REAL_MOVIES = [
    {"title": "Inception", "desc": "A thief who steals corporate secrets through the use of dream-sharing technology.", "rating": "12+", "year": 2010},
    {"title": "The Dark Knight", "desc": "When the menace known as the Joker wreaks havoc and chaos on the people of Gotham.", "rating": "16+", "year": 2008},
    {"title": "Interstellar", "desc": "A team of explorers travel through a wormhole in space in an attempt to ensure humanity's survival.", "rating": "12+", "year": 2014},
    {"title": "The Matrix", "desc": "A computer hacker learns from mysterious rebels about the true nature of his reality.", "rating": "16+", "year": 1999},
    {"title": "Toy Story", "desc": "A cowboy doll is profoundly threatened and jealous when a new spaceman figure supplants him.", "rating": "0+", "year": 1995},
    {"title": "Pulp Fiction", "desc": "The lives of two mob hitmen, a boxer, a gangster and his wife intertwine in four tales.", "rating": "18+", "year": 1994},
    {"title": "Spirited Away", "desc": "A young girl wanders into a world ruled by gods, witches, and spirits.", "rating": "6+", "year": 2001},
    {"title": "Parasite", "desc": "Greed and class discrimination threaten the newly formed symbiotic relationship between families.", "rating": "16+", "year": 2019},
    {"title": "Dune: Part Two", "desc": "Paul Atreides unites with Chani and the Fremen while on a warpath of revenge.", "rating": "12+", "year": 2024},
    {"title": "The Lion King", "desc": "A young lion prince is cast out of his pride by his cruel uncle.", "rating": "0+", "year": 1994}
]

REAL_TRAILERS = [
    "Official Teaser", "Main Trailer", "Final Trailer", "Behind the Scenes", "Director's Cut Preview", "IMAX Special Look"
]

def run_data_generation():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        delete_order = ['Ticket', 'Trailer', 'handles', 'employs', 'Screening', 'Customer', 'Movie', 'Room', 'Manager',
                        'Worker', 'Employee']
        for table in delete_order:
            cursor.execute(f"DELETE FROM {table}")

        for i in range(1, 11):
            cursor.execute("""
                           INSERT INTO Employee (EmployeeID, Name, Salary, Email)
                           VALUES (%s, %s, %s, %s)
                           """, (i, fake.name(), round(random.uniform(2000, 5000), 2), fake.email()))

        for i in range(1, 6):
            dept = 'Marketing' if i == 1 else random.choice(['Operations', 'HR', 'Technical'])
            cursor.execute("""
                           INSERT INTO Manager (EmployeeID, LeadershipExperience, Department)
                           VALUES (%s, %s, %s)
                           """, (i, f"{random.randint(3, 15)} years", dept))

        for i in range(6, 11):
            pos = 'Marketing' if i == 6 else random.choice(['Cleaner', 'Technician', 'Usher'])
            cursor.execute("""
                           INSERT INTO Worker (EmployeeID, Position, WorkingHours)
                           VALUES (%s, %s, %s)
                           """, (i, pos, random.randint(20, 40)))

        for i, movie in enumerate(REAL_MOVIES, 1):
            cursor.execute("""
                           INSERT INTO Movie (MovieID, Title, Description, AgeRating, ThumbnailURL, ReleaseYear)
                           VALUES (%s, %s, %s, %s, %s, %s)
                           """, (i, movie['title'], movie['desc'], movie['rating'],
                                 f"https://picsum.photos/seed/{movie['title']}/400/600", movie['year']))

        for i in range(1, 6):
            cursor.execute("""
                           INSERT INTO Room (RoomID, Capacity, ScreeningType)
                           VALUES (%s, %s, %s)
                           """, (i, random.randint(80, 250), random.choice(['2D', '3D', 'IMAX', '4D'])))

        screening_id = 1
        for movie_id in range(1, 11):
            for _ in range(random.randint(3, 8)):
                room_id = random.randint(1, 5)
                showtime = datetime.now() + timedelta(days=random.randint(-10, 15), hours=random.randint(10, 23))
                available_seats = random.randint(20, 250)
                cursor.execute("""
                               INSERT INTO Screening (ScreeningID, MovieID, RoomID, Showtime, AvailableSeats)
                               VALUES (%s, %s, %s, %s, %s)
                               """, (screening_id, movie_id, room_id, showtime, available_seats))
                screening_id += 1

        for movie_id in range(1, 11):
            num_trailers = random.randint(1, 4)
            selected_trailers = random.sample(REAL_TRAILERS, num_trailers)
            for trailer_id, trailer_desc in enumerate(selected_trailers, 1):
                cursor.execute("""
                               INSERT INTO Trailer (MovieID, TrailerID, URL, Description)
                               VALUES (%s, %s, %s, %s)
                               """,
                               (movie_id, trailer_id, f"https://www.youtube.com/watch?v={fake.lexify('???????????')}",
                                trailer_desc))

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

        for worker_id in range(6, 11):
            rooms = random.sample(range(1, 6), random.randint(1, 4))
            for room_id in rooms:
                cursor.execute("""
                               INSERT IGNORE INTO handles (WorkerID, RoomID)
                               VALUES (%s, %s)
                               """, (worker_id, room_id))

        conn.commit()
        return {"message": "Data imported successfully! All tables cleared and filled with new random data."}
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


def run_mongo_migration():
    mongo_client, db = get_mongo_db()
    try:
        for collection_name in db.list_collection_names():
            db.drop_collection(collection_name)

        sql_conn = get_db_connection()
        cursor = sql_conn.cursor()

        cursor.execute("""
                       SELECT e.EmployeeID,
                              e.Name,
                              e.Salary,
                              e.Email,
                              m.LeadershipExperience,
                              m.Department,
                              w.Position,
                              w.WorkingHours
                       FROM Employee e
                                LEFT JOIN Manager m ON e.EmployeeID = m.EmployeeID
                                LEFT JOIN Worker w ON e.EmployeeID = w.EmployeeID
                       """)
        employees = cursor.fetchall()
        mongo_employees = []
        for emp in employees:
            doc = {
                "employeeID": emp['EmployeeID'],
                "name": emp['Name'],
                "salary": float(emp['Salary']),
                "email": emp['Email'],
            }
            if emp['LeadershipExperience']:
                doc["role"] = "Manager"
                doc["leadershipExperience"] = emp['LeadershipExperience']
                doc["department"] = emp['Department']
            else:
                doc["role"] = "Worker"
                doc["position"] = emp['Position']
                doc["workingHours"] = emp['WorkingHours']
            mongo_employees.append(doc)
        if mongo_employees:
            db.employees.insert_many(mongo_employees)

        cursor.execute("SELECT * FROM Movie")
        movies = cursor.fetchall()
        for movie in movies:
            cursor.execute("SELECT TrailerID, URL, Description FROM Trailer WHERE MovieID = %s", (movie['MovieID'],))
            trailers = cursor.fetchall()
            movie_doc = dict(movie)
            movie_doc["trailers"] = trailers
            db.movies.insert_one(movie_doc)

        cursor.execute("SELECT * FROM Room")
        rooms = cursor.fetchall()
        if rooms:
            db.rooms.insert_many([dict(r) for r in rooms])

        cursor.execute("SELECT * FROM Screening")
        screenings = cursor.fetchall()
        if screenings:
            db.screenings.insert_many([dict(s) for s in screenings])

        cursor.execute("SELECT * FROM Customer")
        customers = cursor.fetchall()
        mongo_customers = []
        for cust in customers:
            cust_doc = dict(cust)
            birth_date = cust_doc.get('BirthDate')
            cust_doc.pop('BirthDate', None)

            if birth_date is None:
                cust_doc['birthDate'] = None
            elif isinstance(birth_date, (date, datetime)):
                cust_doc['birthDate'] = datetime.combine(birth_date, datetime.min.time()) if isinstance(birth_date, date) and not isinstance(birth_date, datetime) else birth_date
            elif isinstance(birth_date, str):
                try:
                    cust_doc['birthDate'] = datetime.strptime(birth_date, '%Y-%m-%d') if len(birth_date) == 10 else datetime.strptime(birth_date, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    cust_doc['birthDate'] = None
            else:
                cust_doc['birthDate'] = None
            mongo_customers.append(cust_doc)

        if mongo_customers:
            db.customers.insert_many(mongo_customers)

        cursor.execute("SELECT * FROM Ticket")
        tickets = cursor.fetchall()
        mongo_tickets = []
        for ticket in tickets:
            ticket_doc = dict(ticket)
            if 'Price' in ticket_doc:
                ticket_doc['price'] = float(ticket_doc['Price'])
                ticket_doc.pop('Price', None)
            mongo_tickets.append(ticket_doc)
        if mongo_tickets:
            db.tickets.insert_many(mongo_tickets)

        cursor.execute("SELECT WorkerID, RoomID FROM handles")
        assignments = cursor.fetchall()
        mongo_assignments = [
            {"workerID": a['WorkerID'], "roomID": a['RoomID'], "assignedAt": datetime.utcnow()}
            for a in assignments
        ]
        if mongo_assignments:
            db.assignments.insert_many(mongo_assignments)

        cursor.close()
        sql_conn.close()
        return {"message": "Migration successful! All collections populated from MariaDB."}
    except Exception as e:
        raise e
    finally:
        mongo_client.close()