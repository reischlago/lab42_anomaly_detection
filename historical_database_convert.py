import sqlite3
import csv
from datetime import datetime

def create_database_schema(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS rooms (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        floor INTEGER NOT NULL,
        number INTEGER NOT NULL,
        UNIQUE(floor, number)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS sensor_data_history (
        room_id INTEGER,
        timestamp DATETIME,
        temperature REAL,
        airquality REAL,
        daylight REAL,
        light INTEGER,
        FOREIGN KEY (room_id) REFERENCES rooms(id)
    )
    ''')
    
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_sensor_data_id ON sensor_data_history(room_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_sensor_data_timestamp ON sensor_data_history(timestamp)')
    
    conn.commit()
    conn.close()

def get_or_create_room_id(cursor, room_str):
    floor = int(room_str[0]) 
    number_str = room_str[1:] 
    
    cursor.execute('SELECT id FROM rooms WHERE floor=? AND number=?', (floor, number_str))
    result = cursor.fetchone()
    
    if result:
        return result[0]
    else:
        cursor.execute('INSERT INTO rooms (floor, number) VALUES (?, ?)', (floor, number_str))
        return cursor.lastrowid
    
def import_csv_to_db(csv_path, db_path, batch_size=1000):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    row_count = 0
    
    with open(csv_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        
        for row in reader:
            room_id = get_or_create_room_id(cursor, row['room'])
            
            try:
                ts = datetime.fromtimestamp(float(row['ts'])).isoformat()
            except ValueError:
                ts = row['ts']
            
            cursor.execute('''
            INSERT INTO sensor_data_history 
            (room_id, timestamp, temperature, airquality, daylight, light)
            VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                room_id,
                ts,
                float(row['temperature']),
                float(row['airquality']),
                float(row['daylight']),
                int(row['light'])
            ))
            
            row_count += 1
            
            if row_count % batch_size == 0:
                conn.commit()
                print(f"Committed {row_count} rows...")
    
    conn.commit()
    print(f"Import complete. Total rows processed: {row_count}")
    conn.close()

if __name__ == "__main__":
    csv_file_path = 'lab42cc_6_new.csv'
    database_path = 'room_data.db'
    
    create_database_schema(database_path)
    
    import_csv_to_db(csv_file_path, database_path)