#!pip install haversine
from haversine import haversine, Unit
import json
import datetime
import psycopg2
import numpy as np
from flask_cors import CORS
from sklearn.cluster import KMeans
from math import sin, cos, sqrt, atan2, radians
from flask import Flask, request, jsonify
# Get current time
current_time = datetime.datetime.now()

# Format current time as a string
formatted_time = current_time.strftime('%Y-%m-%d %H:%M:%S')
app = Flask(__name__)
CORS(app)

def calculate_distance(lat1, lng1, lat2, lng2):
    R = 6371  # radius of the Earth in km

    # convert latitude and longitude coordinates to radians
    lat1, lng1, lat2, lng2 = map(radians, [lat1, lng1, lat2, lng2])

    # calculate the differences in latitude and longitude
    dlat = lat2 - lat1
    dlng = lng2 - lng1

    # apply the Haversine formula
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlng/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    distance = R * c

    return distance
    
def assign_group(lat, lng, speciality, baladia,patient_id):
    import psycopg2
    from sklearn.cluster import KMeans
    from haversine import haversine

    conn = psycopg2.connect(database="sihati", user="sihati",
                            password="Daddy22mars_", host="41.111.206.183", port="5432")
    c = conn.cursor()

    c.execute('SELECT latitude, longitude, group_id, speciality, baladia FROM groups')
    existing_patients = c.fetchall()

    if not existing_patients:
        c.execute('INSERT INTO groups (latitude, longitude, group_id, speciality, baladia,patient_id, created_at,updated_at) VALUES (%s,%s, %s, %s, %s, %s, %s,%s)',
                      (lat, lng, 1, speciality, baladia,patient_id, formatted_time,formatted_time))
        c.execute('INSERT INTO groups_number (group_id, patients_number,created_at,updated_at) VALUES (%s,%s, %s, %s)',
                      ( 1, 1, formatted_time,formatted_time))
        group_id = 1
        c.execute("SELECT group_id FROM patients WHERE id = %s", (patient_id,))
        result = c.fetchone()
        if result:
            group_id_list = result[0]
            if group_id not in group_id_list:
               group_id_list.append(group_id)
               c.execute("UPDATE patients SET group_id = %s WHERE id = %s", (group_id_list, patient_id))
        else:
           c.execute("UPDATE patients SET group_id = %s WHERE id = %s", ([group_id], patient_id))
        
        group_dict = {group_id: []}
    else:
        patient_coords = []
        group_ids = []
        for patient in existing_patients:
            patient_lat, patient_lng, group_id, patient_speciality, patient_baladia = patient
            if patient_speciality == speciality and patient_baladia == baladia:
                patient_coords.append([patient_lat, patient_lng])
                group_ids.append(group_id)

        if not patient_coords:
            max_group_id = max(patient[2] for patient in existing_patients)
            c.execute('INSERT INTO groups (latitude, longitude, group_id, speciality, baladia,patient_id, created_at,updated_at) VALUES (%s,%s, %s, %s, %s, %s, %s, %s)',
                      (lat, lng, max_group_id + 1, speciality, baladia,patient_id, formatted_time,formatted_time))
            c.execute('INSERT INTO groups_number (group_id, patients_number,created_at,updated_at) VALUES (%s,%s, %s, %s)',
                          ( max_group_id + 1, 1, formatted_time,formatted_time))
            group_id = max_group_id + 1
            c.execute("SELECT group_id FROM patients WHERE id = %s", (patient_id,))
            result = c.fetchone()
            if result:
                group_id_list = result[0]
                if group_id not in group_id_list:
                   group_id_list.append(group_id)
                   c.execute("UPDATE patients SET group_id = %s WHERE id = %s", (group_id_list, patient_id))
            else:
               c.execute("UPDATE patients SET group_id = %s WHERE id = %s", ([group_id], patient_id))
            group_dict = {group_id: []}
        else:
            kmeans = KMeans(n_clusters=len(set(group_ids)), random_state=0).fit(patient_coords)

            distances = [haversine((lat, lng), (patient[0], patient[1])) for patient in patient_coords]
            print(distances)
            if all(distance > 30 for distance in distances):
                max_group_id = max(patient[2] for patient in existing_patients)
                c.execute('INSERT INTO groups (latitude, longitude, group_id, speciality, baladia,patient_id,created_at,updated_at) VALUES (%s,%s, %s, %s, %s, %s, %s, %s)',
                      (lat, lng, max_group_id + 1, speciality, baladia,patient_id,formatted_time,formatted_time))
                c.execute('INSERT INTO groups_number (group_id, patients_number,created_at,updated_at) VALUES (%s,%s, %s, %s)',
                          ( max_group_id + 1, 1, formatted_time,formatted_time))

                group_id = max_group_id + 1
                c.execute("SELECT group_id FROM patients WHERE id = %s", (patient_id,))
                result = c.fetchone()
                if result:
                    group_id_list = result[0]
                    if group_id not in group_id_list:
                       group_id_list.append(group_id)
                       c.execute("UPDATE patients SET group_id = %s WHERE id = %s", (group_id_list, patient_id))
                else:
                   c.execute("UPDATE patients SET group_id = %s WHERE id = %s", ([group_id], patient_id))
                group_dict = {group_id: []}
            else:
                closest_group_index = distances.index(min(distances))
                group_id = group_ids[closest_group_index]
                group_dict = {group_id: []}

                c.execute('INSERT INTO groups (latitude, longitude, group_id, speciality, baladia,patient_id,created_at,updated_at) VALUES (%s,%s, %s, %s, %s, %s, %s, %s)',
                      (lat, lng, group_id, speciality, baladia,patient_id,formatted_time,formatted_time))
                c.execute('UPDATE groups_number SET patients_number = patients_number + 1 WHERE group_id = %s', (group_id,))
                c.execute("SELECT group_id FROM patients WHERE id = %s", (patient_id,))
                result = c.fetchone()
                if result:
                    group_id_list = result[0]
                    if group_id not in group_id_list:
                       group_id_list.append(group_id)
                       c.execute("UPDATE patients SET group_id = %s WHERE id = %s", (group_id_list, patient_id))
                else:
                   c.execute("UPDATE patients SET group_id = %s WHERE id = %s", ([group_id], patient_id))

    conn.commit()
    c.close()
    conn.close()

    return group_dict,group_id



@app.route('/assign_group', methods=['POST'])

def assign_group_endpoint():
    data = json.loads(request.data)
    patient_id = data['patient_id']
    lat = data['latitude']
    lng = data['longitude']
    speciality = data['speciality']
    baladia = data['baladia']

    # Call your assign_group function with the extracted data
    group_id, group_dict = assign_group(lat, lng, speciality, baladia,patient_id)

    # Return the response as JSON
    response = {
        'group_id': group_id,
        'group_dict': group_dict
    }
    return jsonify(response)

if __name__ == '__main__':
    app.run(host='0.0.0.0')


