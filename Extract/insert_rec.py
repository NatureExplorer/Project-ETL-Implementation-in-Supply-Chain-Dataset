import psycopg2


def konek_db():
    print('menyambung ke database brok')
    try:
        conn = psycopg2.connect(
            host="db",
            dbname="db",
            user="....",
            password="....",
            port=5432
        )
        return conn
    except psycopg2.Error as e:
        print(f"gagal nyambung ke database: {e}")
        raise

def buat_tabel(conn):
    print('membuat tabel brok')

    try:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE SCHEMA IF NOT EXISTS sustain;
            CREATE TABLE IF NOT EXISTS sustain.customer_service(
                id SERIAL PRIMARY KEY,
                time_id INTEGER,
                Time_Date DATE,
                Time_Year INTEGER,
                Time_Month INTEGER,
                Time_Day INTEGER,
                site_id INTEGER,
                Site_Name TEXT,
                Site_Location TEXT,
                customer_segment_id INTEGER,
                Customer_Segment_Name TEXT,
                Customer_Type TEXT
            );

            CREATE TABLE IF NOT EXISTS sustain.energy_production(
                            id SERIAL PRIMARY KEY,
                            time_id INTEGER,
                            Time_Date DATE,
                            Time_Year INTEGER,
                            Time_Month INTEGER,
                            Time_Day INTEGER,
                            site_id INTEGER,
                            Site_Name TEXT,
                            Site_Location TEXT,
                            energy_type_id INTEGER,
                            Energy_Type_Name TEXT,
                            weather_id INTEGER,
                            Weather_Condition TEXT,
                            Weather_Temperature REAL,
                            equipment_id INTEGER,
                            Equipment_Name TEXT,
                            Equipment_Type TEXT,
                            Equipment_Rated_Capacity REAL,
                            Equipment_Operational_Status TEXT,
                            Electricity_Production FLOAT  
                        );

                CREATE TABLE IF NOT EXISTS sustain.equipment_performance(
                            id SERIAL PRIMARY KEY,
                            time_id INTEGER,
                            Time_Date DATE,
                            Time_Year INTEGER,
                            Time_Month INTEGER,
                            Time_Day INTEGER,
                            site_id INTEGER,
                            Site_Name TEXT,
                            Site_Location TEXT,
                            equipment_id INTEGER,
                            Equipment_Name TEXT,
                            Equipment_Type TEXT,
                            Equipment_Rated_Capacity REAL,
                            Equipment_Operational_Status TEXT,
                            energy_type_id INTEGER,
                            Energy_Type_Name TEXT,
                            weather_id INTEGER,
                            Weather_Condition TEXT,
                            Weather_Temperature FLOAT);

                CREATE TABLE IF NOT EXISTS sustain.regulatory_compliance_data.csv(
                            id SERIAL PRIMARY KEY,
                            time_id INTEGER,
                            Time_Date DATE,
                            Time_Year INTEGER,
                            Time_Month INTEGER,
                            Time_Day INTEGER,
                            site_id INTEGER,
                            Site_Name TEXT,
                            Site_Location TEXT,
                            regulation_id INTEGER,
                            Regulation_Name TEXT,
                            Regulation_Authority TEXT);         
        ''')
        print('tabel berhasil dibuat brok')

    except psycopg2.Error as e:
        print(f"gagal membuat tabel: {e}")
        raise


def insert_data(conn, data):
    print('memasukkan data brok')
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO sustain.customer_service (time_id, Time_Date, Time_Year, Time_Month, Time_Day, site_id, Site_Name, Site_Location, customer_segment_id, Customer_Segment_Name, Customer_Type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            data['location']['name'],
            data['current']['temperature'],
            data['current']['weather_descriptions'][0],
            data['current']['wind_speed'],
            data['location']['localtime'],
            float(data['location']['utc_offset'])
        ))
        print('data berhasil dimasukkan brok')

    except psycopg2.Error as e:
        print(f"gagal memasukkan data: {e}")
        raise


def main():
    try:
        conn = konek_db()
        buat_tabel(conn)
        data = dapet_data()
        insert_data(conn, data)
        print('selesai brok')
    except Exception as e:
        print(f"gagal melakukan operasi: {e}")
        if 'conn' in locals():
            conn.rollback()
        raise
    else:
        conn.commit()
    finally:
        if 'conn' in locals():
            conn.close()
            print('koneksi ditutup brok')

