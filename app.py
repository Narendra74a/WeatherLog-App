import sqlite3
from datetime import datetime
import csv
from dotenv import load_dotenv
import os
import requests

load_dotenv()

db_url = os.getenv("DATABASE_URL")
secret_key = os.getenv("SECRET_KEY")


def init_db():
    connect= sqlite3.connect(db_url )
    c=connect.cursor()

    # //create table
    c.execute( ''' 
    create table if not exists weather_Log (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              city text,
              temp Real,
              condition TEXT,
              date_time TEXT
              )
              ''' )
    
    connect.commit()
    connect.close()





def fetch_weather(city,api_key):
    url=f'https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric'
    res=requests.get(url)
    data=res.json()

    if res.status_code!=200 or 'main' not in data:
        print("Error: not fetch weather. Check city name or API key.")
        return None
    
    temperature = data['main']['temp'] 
    condition = data['weather'][0]['description']
    return temperature, condition







def saveWeather_data (city, temp,condition):
    conn= sqlite3.connect('weatherLog.db')
    c = conn.cursor()

    date_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    c.execute('''
        INSERT INTO weather_Log(city, temp, condition, date_time)
        VALUES (?, ?, ?, ?)
    ''', (city, temp, condition, date_time))

    conn.commit()
    conn.close()
    print(f"Weather log saved for {city} at {date_time}")





def view_all_data():
     conn= sqlite3.connect('weatherLog.db')
     c = conn.cursor()

     c.execute('SELECT * FROM weather_Log')
     rows=c.fetchall()

     if not rows:
        print("No logs found.")
     else:
        print("\nAll Weather Logs:")
        for row in rows:
            print(f"ID: {row[0]}, City: {row[1]}, Temp: {row[2]}°C, Condition: {row[3]}, DateTime: {row[4]}")

     conn.close()




    
def filter_data(city=None, date=None):
    conn = sqlite3.connect('weatherLog.db')
    c = conn.cursor()

    if city:
        print(f"\nFiltering logs for city: {city}")
        c.execute('SELECT * FROM  weather_Log WHERE city = ?', (city,))
    elif date:
        print(f"\nFiltering logs for date: {date}")
        c.execute('SELECT * FROM weather_Log WHERE date_time LIKE ?', (f'{date}%',))
    else:
        print("No filter provided. Showing all logs.")
        c.execute('SELECT * FROM weather_Log')


    rows = c.fetchall()

    if not rows:
        print("No logs found with the given filter.")
    else:
        print("\nFiltered Logs:")
        for row in rows:
            print(f"ID: {row[0]}, City: {row[1]}, Temp: {row[2]}°C, Condition: {row[3]}, DateTime: {row[4]}")

    conn.close()







def delete_data_by_id(log_id):
    conn = sqlite3.connect('weatherLog.db')
    c = conn.cursor()

    c.execute('DELETE FROM weather_Log WHERE id = ?', (log_id,))
    conn.commit()

    if c.rowcount == 0:
        print(f"No log found with ID {log_id}.")
    else:
        print(f"Data with ID {log_id} deleted successfully.")

    conn.close()    





def export_data_to_csv(filename):
    conn = sqlite3.connect('weatherLog.db')
    c = conn.cursor()

    c.execute('SELECT * FROM weather_Log')
    rows = c.fetchall()

    if not rows:
        print("No logs found. Nothing to export.")
        conn.close()
        return

    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['ID', 'City', 'Temperature', 'Condition', 'Date_Time'])  # Header
        writer.writerows(rows)

    conn.close()
    print(f"Logs exported successfully to {filename}")



if __name__ == "__main__":
    init_db()

    api_key =  secret_key

    while True:
        print("\nWeather Log App - Main Menu.")
        print("1️⃣ Fetch and Save Current Weather")
        print("2️⃣ View All Logs")
        print("3️⃣ Filter Logs")
        print("4️⃣ Delete Log by ID")
        print("5️⃣ Export Logs to CSV")
        print("6️⃣ Exit")

        choice = input("\nEnter your choice (1-6): ").strip()

        if choice == '1':
            city = input("Enter city name: ").strip()
            if not city:
                print("City name cannot be empty.")
                continue

            result = fetch_weather(city, api_key)
            if result:
                temperature, condition = result
                print(f"Weather in {city}: {temperature}°C, {condition}")
                saveWeather_data(city, temperature, condition)

        elif choice == '2':
            view_all_data()

        elif choice == '3':
            city_input = input("Enter city name to filter (or leave blank): ").strip()
            date_input = input("Enter date (YYYY-MM-DD) to filter (or leave blank): ").strip()

            city = city_input if city_input else None
            date = date_input if date_input else None

            filter_data(city, date)

        elif choice == '4':
            view_all_data()
            log_id_input = input("\nEnter the ID of the log to delete: ").strip()
            if log_id_input:
                try:
                    log_id = int(log_id_input)
                    delete_data_by_id(log_id)
                except ValueError:
                    print("Invalid ID. Must be an integer.")

        elif choice == '5':
            filename = input("Enter filename for CSV export (e.g., logs.csv): ").strip()
            if filename:
                export_data_to_csv(filename)
            else:
                print("Filename cannot be empty.")

        elif choice == '6':
            print("Exiting Weather Log App. Goodbye!")
            break

        else:
            print("Invalid choice. Please select 1-6.")
