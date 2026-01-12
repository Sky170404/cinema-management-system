from flask import jsonify
from db_utils import get_db_connection, get_mongo_db
from faker import Faker
import random
from datetime import datetime, timedelta, date

fake = Faker()

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
            cursor.execute("""
                           INSERT INTO Manager (EmployeeID, LeadershipExperience, Department)
                           VALUES (%s, %s, %s)
                           """, (i, f"{random.randint(3, 15)} years", random.choice(['Operations', 'HR', 'Technical'])))

        for i in range(6, 11):
            cursor.execute("""
                           INSERT INTO Worker (EmployeeID, Position, WorkingHours)
                           VALUES (%s, %s, %s)
                           """, (i, random.choice(['Cleaner', 'Technician', 'Marketing']), random.randint(20, 40)))

        for i in range(1, 11):
            cursor.execute("""
                           INSERT INTO Movie (MovieID, Title, Description, AgeRating, ThumbnailURL, ReleaseYear)
                           VALUES (%s, %s, %s, %s, %s, %s)
                           """, (i, fake.sentence(nb_words=5)[:-1], fake.paragraph(),
                                 random.choice(['0+', '6+', '12+', '16+', '18+']), fake.image_url(),
                                 random.randint(2015, 2025)))

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