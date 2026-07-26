"""
Generates a small, internally-consistent synthetic healthcare dataset
matching the Bronze layer's source tables (states, cities, ...
hospital_visits). Output: CSVs in ./synthetic_data/, ready to upload to
a Databricks volume and load into bronze.* (see
databricks/notebooks/01_bronze_ingest.py).
"""

import csv
import random
from datetime import date, timedelta
from pathlib import Path
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

OUT_DIR = Path("synthetic_data")
OUT_DIR.mkdir(exist_ok=True)

# ---------------------------------------------------------------------
# Reference volumes (kept small/readable for a portfolio project)
# ---------------------------------------------------------------------
N_STATES = 8
N_CITIES = 20
N_ADDRESSES = 30
N_HOSPITALS = 10
N_DOCTORS = 30
N_MEDICATIONS = 20
N_PROCEDURES = 15
N_INSURANCE_PROVIDERS = 8
N_PATIENTS = 250
N_PRESCRIPTIONS = 600
N_HOSPITAL_VISITS = 350

DATE_START = date(2022, 1, 1)
DATE_END = date(2025, 12, 31)


def random_date(start=DATE_START, end=DATE_END):
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def write_csv(filename, header, rows):
    path = OUT_DIR / filename
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"  wrote {filename}: {len(rows)} rows")


# ---------------------------------------------------------------------
# STATES
# ---------------------------------------------------------------------
us_states = [
    ("California", "CA"), ("Texas", "TX"), ("Florida", "FL"), ("New York", "NY"),
    ("Illinois", "IL"), ("Pennsylvania", "PA"), ("Ohio", "OH"), ("Georgia", "GA"),
]
states_rows = [(i + 1, name, abbr) for i, (name, abbr) in enumerate(us_states[:N_STATES])]
write_csv("states.csv", ["state_id", "state_name", "state_abbr"], states_rows)
state_ids = [r[0] for r in states_rows]

# ---------------------------------------------------------------------
# CITIES
# ---------------------------------------------------------------------
cities_rows = [(i + 1, fake.city(), random.choice(state_ids)) for i in range(N_CITIES)]
write_csv("cities.csv", ["city_id", "city_name", "state_id"], cities_rows)
city_ids = [r[0] for r in cities_rows]

# ---------------------------------------------------------------------
# ADDRESSES
# ---------------------------------------------------------------------
addresses_rows = [
    (i + 1, fake.street_address(), random.choice(city_ids), fake.postcode())
    for i in range(N_ADDRESSES)
]
write_csv("addresses.csv", ["address_id", "street_address", "city_id", "zip"], addresses_rows)
address_ids = [r[0] for r in addresses_rows]

# ---------------------------------------------------------------------
# HOSPITALS
# ---------------------------------------------------------------------
hospital_suffixes = ["General Hospital", "Medical Center", "Regional Hospital", "Health System", "Clinic & Hospital"]
hospitals_rows = [
    (i + 1, f"{fake.city()} {random.choice(hospital_suffixes)}", random.choice(address_ids), fake.phone_number())
    for i in range(N_HOSPITALS)
]
write_csv("hospitals.csv", ["hospital_id", "hospital_name", "address_id", "hospital_phone_no"], hospitals_rows)
hospital_ids = [r[0] for r in hospitals_rows]

# ---------------------------------------------------------------------
# DOCTORS
# ---------------------------------------------------------------------
specialties = ["Cardiology", "Pediatrics", "Oncology", "Neurology", "Orthopedics",
               "Internal Medicine", "Family Medicine", "Dermatology", "Psychiatry", "General Surgery"]
doctors_rows = [
    (i + 1, fake.first_name(), fake.last_name(), random.choice(hospital_ids),
     random.choice(specialties), fake.phone_number())
    for i in range(N_DOCTORS)
]
write_csv("doctors.csv", ["doctor_id", "first_name", "last_name", "hospital_affi", "specialty", "doc_phone_no"], doctors_rows)
doctor_ids = [r[0] for r in doctors_rows]

# ---------------------------------------------------------------------
# MEDICATIONS
# ---------------------------------------------------------------------
med_names = ["Lisinopril", "Metformin", "Amlodipine", "Metoprolol", "Omeprazole",
             "Simvastatin", "Losartan", "Albuterol", "Gabapentin", "Hydrochlorothiazide",
             "Sertraline", "Atorvastatin", "Levothyroxine", "Amoxicillin", "Ibuprofen",
             "Prednisone", "Furosemide", "Insulin Glargine", "Warfarin", "Clopidogrel"]
pharma_companies = ["Pfizer", "Novartis", "Merck", "Sanofi", "GSK", "AbbVie", "Teva", "Bayer"]
categories = ["Cardiovascular", "Diabetes", "Respiratory", "Pain Relief", "Antibiotic", "Mental Health", "Gastrointestinal"]
medications_rows = [
    (i + 1, med_names[i % len(med_names)], random.choice(pharma_companies),
     random.choice(categories), round(random.uniform(5, 250), 2))
    for i in range(N_MEDICATIONS)
]
write_csv("medications.csv", ["medication_id", "medication_name", "pharma_company", "category", "medication_cost"], medications_rows)
medication_ids = [r[0] for r in medications_rows]
medication_cost_by_id = {r[0]: r[4] for r in medications_rows}

# ---------------------------------------------------------------------
# PROCEDURES
# ---------------------------------------------------------------------
proc_names = ["Appendectomy", "Colonoscopy", "MRI Scan", "CT Scan", "X-Ray",
              "Blood Test Panel", "Cardiac Catheterization", "Hip Replacement",
              "Knee Replacement", "Cataract Surgery", "Gallbladder Removal",
              "Endoscopy", "Biopsy", "Physical Therapy Session", "EKG"]
med_categories = ["Surgical", "Diagnostic", "Imaging", "Laboratory", "Therapy"]
procedures_rows = [
    (i + 1, proc_names[i % len(proc_names)], random.choice(med_categories), round(random.uniform(100, 15000), 2))
    for i in range(N_PROCEDURES)
]
write_csv("procedures.csv", ["procedure_id", "procedure_name", "medical_category", "procedure_charge"], procedures_rows)
procedure_ids = [r[0] for r in procedures_rows]

# ---------------------------------------------------------------------
# INSURANCE PROVIDERS
# ---------------------------------------------------------------------
insurers = ["Blue Cross Blue Shield", "UnitedHealthcare", "Aetna", "Cigna",
            "Humana", "Kaiser Permanente", "Anthem", "Molina Healthcare"]
plan_types = ["HMO", "PPO", "EPO", "POS"]
insurance_rows = [
    (i + 1, insurers[i % len(insurers)], random.choice(plan_types), fake.phone_number())
    for i in range(N_INSURANCE_PROVIDERS)
]
write_csv("insurance_providers.csv", ["insurance_provider_id", "provider_name", "plan_type", "phone_no"], insurance_rows)
insurance_ids = [r[0] for r in insurance_rows]

# ---------------------------------------------------------------------
# PATIENTS
# ---------------------------------------------------------------------
blood_types = ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]
patients_rows = []
for i in range(N_PATIENTS):
    dob = fake.date_of_birth(minimum_age=1, maximum_age=95)
    sex = random.choice(["M", "F"])
    first = fake.first_name_male() if sex == "M" else fake.first_name_female()
    patients_rows.append((i + 1, first, fake.last_name(), dob, sex, random.choice(blood_types)))
write_csv("patients.csv", ["patient_id", "first_name", "last_name", "dob", "sex", "blood_type"], patients_rows)
patient_ids = [r[0] for r in patients_rows]

# ---------------------------------------------------------------------
# PATIENT_INSURANCE (each patient has 1, most have 2 -- primary + secondary)
# ---------------------------------------------------------------------
patient_insurance_rows = []
for pid in patient_ids:
    n_policies = 1 if random.random() < 0.4 else 2
    chosen = random.sample(insurance_ids, k=min(n_policies, len(insurance_ids)))
    for ins_id in chosen:
        patient_insurance_rows.append((pid, ins_id))
write_csv("patient_insurance.csv", ["patient_id", "insurance_provider_id"], patient_insurance_rows)

# ---------------------------------------------------------------------
# PRESCRIPTIONS
# ---------------------------------------------------------------------
dosage_options = ["5mg", "10mg", "20mg", "25mg", "50mg", "100mg", "250mg", "500mg"]
frequency_options = ["Once daily", "Twice daily", "Three times daily", "As needed", "Every 12 hours"]
prescriptions_rows = []
for i in range(N_PRESCRIPTIONS):
    refill_allowed = random.random() < 0.7
    prescriptions_rows.append((
        i + 1,
        random.choice(patient_ids),
        random.choice(doctor_ids),
        random.choice(medication_ids),
        random.randint(1, 4),
        random.choice(dosage_options),
        random.choice(frequency_options),
        random_date(),
        refill_allowed,
        random.randint(0, 5) if refill_allowed else 0,
    ))
write_csv(
    "prescriptions.csv",
    ["prescription_id", "patient_id", "doctor_id", "medication_id", "quantity",
     "dosage", "frequency", "prescribed_date", "refill_allowed", "refill_count"],
    prescriptions_rows,
)

# ---------------------------------------------------------------------
# HOSPITAL_VISITS
# ---------------------------------------------------------------------
admission_types = ["Emergency", "Elective", "Urgent", "Newborn"]
diagnoses = ["Hypertension", "Type 2 Diabetes", "Pneumonia", "Fracture", "Appendicitis",
             "Chest Pain", "Asthma Exacerbation", "Migraine", "Kidney Stones",
             "Gastroenteritis", "Congestive Heart Failure", "COPD Exacerbation"]
hospital_visits_rows = []
for i in range(N_HOSPITAL_VISITS):
    admission = random_date()
    stay_length = random.choices([0, 1, 2, 3, 5, 7, 14], weights=[20, 25, 20, 15, 10, 7, 3])[0]
    discharge = admission + timedelta(days=stay_length)
    if discharge > DATE_END:
        discharge = DATE_END
    hospital_visits_rows.append((
        i + 1,
        admission,
        discharge,
        random.choice(patient_ids),
        random.choice(doctor_ids),
        random.choice(hospital_ids),
        random.choice(insurance_ids),
        str(random.randint(100, 499)),
        random.choice(admission_types),
        random.choice(procedure_ids),
        random.choice(diagnoses),
    ))
write_csv(
    "hospital_visits.csv",
    ["visit_id", "admission_date", "discharge_date", "patient_id", "doctor_id",
     "hospital_id", "insurance_provider_id", "room_no", "admission_type",
     "procedure_id", "diagnosis"],
    hospital_visits_rows,
)

print("\nDone. All CSVs are in ./synthetic_data/")
