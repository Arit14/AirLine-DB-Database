/*
Airine DB Database

Skills used: Joins, CTE's, Temp Tables, Windows Functions, Aggregate Functions, Creating Views, Converting Data Types

*/


1. Represent the “book_date” column in “yyyy-mmm-dd” format using Bookings table 
Expected output: book_ref, book_date (in “yyyy-mmm-dd” format) , total amount 
Answer:  SELECT 
    book_ref, 
    DATE_FORMAT(book_date, '%Y-%b-%d') AS formatted_book_date, 
    total_amount
FROM 
    Bookings;

2. Get the following columns in the exact same sequence.
Expected columns in the output: ticket_no, boarding_no, seat_number, passenger_id, passenger_name.
Answer:  SELECT 
    BP.ticket_no,
    BP.boarding_no,
    BP.seat_no,
    T.passenger_id,
    T.passenger_name
FROM 
    BOARDING_PASSES BP
JOIN 
    TICKETS T ON BP.ticket_no = T.ticket_no;

3. Write a query to find the seat number which is least allocated among all the seats?

Answer:  SELECT 
    seat_no,
    COUNT(*) AS allocation_count
FROM 
    BOARDING_PASSES
GROUP BY 
    seat_no
ORDER BY 
    allocation_count ASC
LIMIT 1;
4. In the database, identify the month wise highest paying passenger name and passenger id.
Expected output: Month_name(“mmm-yy” format), passenger_id, passenger_name and total amount


Answer: WITH PassengerTotalAmount AS (
    SELECT 
        T.passenger_id,
        T.passenger_name,
        DATE_FORMAT(B.book_date, '%b-%y') AS month_name,
        SUM(B.total_amount) AS total_amount,
        ROW_NUMBER() OVER (PARTITION BY DATE_FORMAT(B.book_date, '%b-%y') ORDER BY SUM(B.total_amount) DESC) AS rank
    FROM 
        TICKETS T
    JOIN 
        BOOKINGS B ON T.book_ref = B.book_ref
    GROUP BY 
        T.passenger_id, T.passenger_name, DATE_FORMAT(B.book_date, '%b-%y')
)
SELECT 
    month_name,
    passenger_id,
    passenger_name,
    total_amount
FROM 
    PassengerTotalAmount
WHERE 
    rank = 1;


5. In the database, identify the month wise least paying passenger name and passenger id?
Expected output: Month_name(“mmm-yy” format), passenger_id, passenger_name and total amount

Answer:  WITH PassengerTotalAmount AS (
    SELECT 
        T.passenger_id,
        T.passenger_name,
        DATE_FORMAT(B.book_date, '%b-%y') AS month_name,
        SUM(B.total_amount) AS total_amount,
        ROW_NUMBER() OVER (PARTITION BY DATE_FORMAT(B.book_date, '%b-%y') ORDER BY SUM(B.total_amount) ASC) AS rank
    FROM 
        TICKETS T
    JOIN 
        BOOKINGS B ON T.book_ref = B.book_ref
    GROUP BY 
        T.passenger_id, T.passenger_name, DATE_FORMAT(B.book_date, '%b-%y')
)
SELECT 
    month_name,
    passenger_id,
    passenger_name,
    total_amount
FROM 
    PassengerTotalAmount
WHERE 
    rank = 1;

6. Identify the travel details of non stop journeys  or return journeys (having more than 1 flight).
Expected Output: Passenger_id, passenger_name, ticket_number and flight count.

Answer:  SELECT 
    T.passenger_id,
    T.passenger_name,
    T.ticket_number,
    COUNT(BP.ticket_no) AS flight_count
FROM 
    TICKETS T
JOIN 
    BOARDING_PASSES BP ON T.ticket_no = BP.ticket_no
GROUP BY 
    T.passenger_id, T.passenger_name, T.ticket_number
HAVING 
    COUNT(BP.ticket_no) > 1;

7. How many tickets are there without boarding passes?
Expected Output: just one number is required.

Answer:  SELECT COUNT(T.ticket_no)
FROM TICKETS T
LEFT JOIN BOARDING_PASSES BP ON T.ticket_no = BP.ticket_no
WHERE BP.ticket_no IS NULL;


8. Identify details of the longest flight (using flights table)?
Expected Output: Flight number, departure airport, arrival airport, aircraft code and durations.

Answer: SELECT
    flight_no,
    departure_airport,
    arrival_airport,
    aircraft_code,
    TIMESTAMPDIFF(MINUTE, scheduled_departure, scheduled_arrival) AS duration_minutes
FROM
    flights
ORDER BY
    duration_minutes DESC
LIMIT 1;



9. Identify details of all the morning flights (morning means between 6AM to 11 AM, using flights table)?
Expected output: flight_id, flight_number, scheduled_departure, scheduled_arrival and timings.

Answer:  SELECT
    flight_id,
    flight_no,
    scheduled_departure,
    scheduled_arrival,
    'Morning' AS timings
FROM
    flights
WHERE
    EXTRACT(HOUR FROM scheduled_departure) BETWEEN 6 AND 10;




10. Identify the earliest morning flight available from every airport.
Expected output: flight_id, flight_number, scheduled_departure, scheduled_arrival, departure airport and timings.
Answer:  SELECT
    flight_id,
    flight_no,
    scheduled_departure,
    scheduled_arrival,
    departure_airport,
    'Earliest Morning' AS timings
FROM
    flights
WHERE
    EXTRACT(HOUR FROM scheduled_departure) BETWEEN 2 AND 5;

11. Questions: Find list of airport codes in Europe/Moscow timezone
 Expected Output:  Airport_code. 

Answer:  SELECT
    airport_code
FROM AIRPORTS
WHERE timezone IN ('Europe/Moscow')

12. Write a query to get the count of seats in various fare condition for every aircraft code?
 Expected Outputs: Aircraft_code, fare_conditions ,seat count

Answer:  SELECT
    aircraft_code,
    fare_conditions,
    COUNT(seat_no)
FROM SEATS
GROUP BY 1,2

13. How many aircrafts codes have at least one Business class seats?
 Expected Output : Count of aircraft codes

Answer:  SELECT
    COUNT(aircraft_code)
FROM SEATS
WHERE fare_conditions = 'Business'

14. Find out the name of the airport having maximum number of departure flight
 Expected Output : Airport_name 

Answer:  WITH DepartureFlightCounts AS (
    SELECT
        departure_airport,
        COUNT(*) AS departure_count
    FROM
        flights
    GROUP BY
        departure_airport
)
SELECT
    a.airport_name
FROM
    airports a
JOIN
    DepartureFlightCounts dfc ON a.airport_code = dfc.departure_airport
ORDER BY
    dfc.departure_count DESC
LIMIT 1;

15. Find out the name of the airport having least number of scheduled departure flights
 Expected Output : Airport_name 

Answer:  SELECT airport_name
FROM airports
LEFT JOIN (
    SELECT departure_airport, COUNT(*) AS departure_count
    FROM flights
    GROUP BY departure_airport
) AS departure_counts ON airports.airport_code = departure_counts.departure_airport
ORDER BY COALESCE(departure_counts.departure_count, 0)
LIMIT 1;

16. How many flights from ‘DME’ airport don’t have actual departure?
 Expected Output : Flight Count 

Answer:   SELECT COUNT(flight_id)
FROM flights
WHERE departure_airport = 'DME'
AND actual_departure IS NULL;

17. Identify flight ids having range between 3000 to 6000
 Expected Output : Flight_Number , aircraft_code, ranges 

Answer:  SELECT f.flight_no, a.aircraft_code, a.range
FROM FLIGHTS f
JOIN AIRCRAFTS a ON f.aircraft_code = a.aircraft_code
WHERE a.range BETWEEN 3000 AND 6000
GROUP BY 1,2,3


18. Write a query to get the count of flights flying between URS and KUF?
 Expected Output : Flight_count

Answer:  SELECT
    COUNT(flight_id) as flight_count
FROM flights
WHERE departure_airport = 'URS' AND arrival_airport = 'KUF'


19. Write a query to get the count of flights flying from either from NOZ or KRR?
 Expected Output : Flight count 

Answer:   SELECT COUNT(*) AS Flight_count
FROM flights
WHERE departure_airport IN ('NOZ', 'KRR');


20. Write a query to get the count of flights flying from KZN,DME,NBC,NJC,GDX,SGC,VKO,ROV
Expected Output : Departure airport ,count of flights flying from these   airports.

Answer:  SELECT departure_airport, COUNT(*) AS Flight_count
FROM flights
WHERE departure_airport IN ('KZN', 'DME', 'NBC', 'NJC', 'GDX', 'SGC', 'VKO', 'ROV')
GROUP BY departure_airport;


21. Write a query to extract flight details having range between 3000 and 6000 and flying from DME
Expected Output :Flight_no,aircraft_code,range,departure_airport

Answer:  SELECT f.Flight_no, f.aircraft_code, a.range, f.departure_airport
FROM FLIGHTS f
JOIN AIRCRAFTS a ON f.aircraft_code = a.aircraft_code
WHERE a.range BETWEEN 3000 AND 6000
AND f.departure_airport = 'DME';

22. Find the list of flight ids which are using aircrafts from “Airbus” company and got cancelled or delayed
 Expected Output : Flight_id,aircraft_model

Answer:  SELECT f.flight_id, a.model
FROM flights f
JOIN aircrafts a ON f.aircraft_code = a.aircraft_code
WHERE a.model LIKE '%Airbus%'
AND (f.status = 'Cancelled' OR f.status = 'Delayed');


23. Find the list of flight ids which are using aircrafts from “Boeing” company and got cancelled or delayed
Expected Output : Flight_id,aircraft_model

Answer:  SELECT f.flight_id, a.model
FROM flights f
JOIN aircrafts a ON f.aircraft_code = a.aircraft_code
WHERE a.model LIKE '%Boeing%'
AND (f.status = 'Cancelled' OR f.status = 'Delayed');


24. Which airport(name) has most cancelled flights (arriving)?
Expected Output : Airport_name 
Answer:  SELECT airports.airport_name
FROM airports
JOIN flights ON airports.airport_code = flights.arrival_airport
WHERE flights.status = 'Cancelled'
ORDER BY 1 DESC
LIMIT 1;
25. Identify flight ids which are using “Airbus aircrafts”
Expected Output : Flight_id,aircraft_model

Answer:  SELECT
    F.flight_id,
    A.model
FROM FLIGHTS F
JOIN AIRCRAFTS A
ON F.aircraft_code = A.aircraft_code
WHERE A.model LIKE '%Airbus%'
GROUP BY 1,2


26. Identify date-wise last flight id flying from every airport?
Expected Output: Flight_id,flight_number,schedule_departure,departure_airport

Answer:  SELECT f1.flight_id, f1.flight_no, f1.scheduled_departure, f1.departure_airport
FROM flights f1
JOIN (
    SELECT departure_airport, MAX(scheduled_departure) AS max_schedule_departure
    FROM flights
    GROUP BY departure_airport
) f2 ON f1.departure_airport = f2.departure_airport
    AND f1.scheduled_departure = f2.max_schedule_departure;


27. Identify list of customers who will get the refund due to cancellation of the flights and how much amount they will get?
Expected Output : Passenger_name,total_refund.

Answer:  SELECT T.Passenger_name, SUM(TF.amount) AS total_refund
FROM TICKET_FLIGHTS TF
JOIN TICKETS T ON TF.ticket_no = T.ticket_no
JOIN flights f ON TF.flight_id = f.flight_id
WHERE f.status = 'Cancelled'
GROUP BY T.ticket_no, T.Passenger_name;


28. Identify date wise first cancelled flight id flying for every airport?
Expected Output : Flight_id,flight_number,schedule_departure,departure_airport

Answer:  SELECT Flight_id, flight_no, scheduled_departure, departure_airport
FROM (
    SELECT f.Flight_id, f.flight_no, f.scheduled_departure, f.departure_airport,
           ROW_NUMBER() OVER (PARTITION BY f.departure_airport ORDER BY f.scheduled_departure) AS flight_rank
    FROM flights f
    WHERE f.status = 'Cancelled'
) AS ranked_flights
WHERE flight_rank = 1;

29. Identify list of Airbus flight ids which got cancelled.
Expected Output : Flight_id

Answer:   SELECT f.flight_id
FROM flights f
JOIN aircrafts a ON f.aircraft_code = a.aircraft_code
WHERE a.model LIKE '%Airbus%'
AND f.status = 'Cancelled';

30. Identify list of flight ids having highest range.
 Expected Output : Flight_no, range


Answer:  SELECT f.Flight_id, a.range
FROM flights f
JOIN AIRCRAFTS a
ON f.aircraft_code = a.aircraft_code
ORDER BY range DESC
LIMIT 1;
