CREATE TABLE Employee (
    EmployeeID INT PRIMARY KEY,
    Name VARCHAR(255),
    Salary DECIMAL(10,2),
    Email VARCHAR(255)
);


CREATE TABLE Manager (
    EmployeeID INT PRIMARY KEY,
    LeadershipExperience VARCHAR(255),
    Department VARCHAR(50),
    FOREIGN KEY (EmployeeID) REFERENCES Employee(EmployeeID) ON DELETE CASCADE
);


CREATE TABLE Worker (
    EmployeeID INT PRIMARY KEY,
    Position VARCHAR(255),
    WorkingHours INT,
    FOREIGN KEY (EmployeeID) REFERENCES Employee(EmployeeID) ON DELETE CASCADE
);


CREATE TABLE Movie (
    MovieID INT PRIMARY KEY,
    Title VARCHAR(255),
    Description VARCHAR(2000),
    AgeRating VARCHAR(20),
    ThumbnailURL VARCHAR(500),
    ReleaseYear INT
);


CREATE TABLE Room (
    RoomID INT PRIMARY KEY,
    Capacity INT,
    ScreeningType VARCHAR(50),
    CONSTRAINT check_screeningType CHECK(ScreeningType IN('3D', 'IMAX', '4D', '2D'))
);


CREATE TABLE Screening (
    ScreeningID INT PRIMARY KEY,
    MovieID INT NOT NULL,
    RoomID INT NOT NULL,
    Showtime DATETIME NOT NULL,
    AvailableSeats INT,
    FOREIGN KEY (MovieID) REFERENCES Movie(MovieID) ON DELETE CASCADE,
    FOREIGN KEY (RoomID) REFERENCES Room(RoomID) ON DELETE CASCADE
);


CREATE TABLE Customer (
    CustomerID INT PRIMARY KEY,
    Name VARCHAR(255),
    BirthDate DATE,
    PaymentReference VARCHAR(50) NOT NULL,
    CONSTRAINT check_paymentreference CHECK(PaymentReference IN('creditcard', 'PayPal', 'cash'))
);


CREATE TABLE Ticket (
    TicketID INT PRIMARY KEY,
    ScreeningID INT NOT NULL,
    Price DECIMAL(10,2),
    Seat INT NOT NULL,
    CustomerID INT,
    FOREIGN KEY (ScreeningID) REFERENCES Screening(ScreeningID) ON DELETE CASCADE,
    FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID) ON DELETE SET NULL
);


CREATE TABLE Trailer (
    MovieID INT NOT NULL,
    TrailerID INT,
    URL VARCHAR(255),
    Description VARCHAR(500),
    PRIMARY KEY (MovieID, TrailerID),
    FOREIGN KEY (MovieID) REFERENCES Movie(MovieID) ON DELETE CASCADE
);


CREATE TABLE handles (
    WorkerID INT,
    RoomID INT,
    PRIMARY KEY (WorkerID, RoomID),
    FOREIGN KEY (WorkerID) REFERENCES Worker(EmployeeID) ON DELETE CASCADE,
    FOREIGN KEY (RoomID) REFERENCES Room(RoomID) ON DELETE CASCADE
);


CREATE TABLE employs (
    EmployerID INT,
    EmployeeID INT,
    PRIMARY KEY (EmployerID, EmployeeID),
    FOREIGN KEY (EmployerID) REFERENCES Worker(EmployeeID) ON DELETE CASCADE,
    FOREIGN KEY (EmployeeID) REFERENCES Worker(EmployeeID) ON DELETE CASCADE,
    CHECK (EmployerID <> EmployeeID)
);
