import random
import json
from faker import Faker

faker = Faker()


DECLARE = {
    "username":{"global_key":"true", "unique":"true"},
    "student_id":{"unique":"true"}, # student, takes, advisor
    "instructor_id":{"unique":"true"}, # instructor, teaches
    "time_slot_id":{"unique":"true"}, # time_slot
    "course_id":{"unique":"true"}, # course, prereq
    "dept_name":{"unique":"true"}, # instructor, student, course
    "sec_id":{},
    "title":{},
    "credits":{},
    "building":{},
    "room_no":{},
    "tot_cred":{},
    "student_name":{},
    "instructor_name":{},
    "semester":{},
    "year":{},
    "day":{},
    "start_time":{},
    "end_time":{},
    "capacity":{},
    "grade":{},
    "prereq_id":{},
    "salary":{},
    "budget":{}
}

DEPT_POOL = [] # pools to add created records in
COURSE_POOL = []
CLASSROOM_POOL = []
TIMESLOT_POOL = []
SECTION_POOL = []
STUDENT_POOL = []
INSTRUCTOR_POOL = []
# ADVISOR, PREREQ, TAKES, TEACHES, these are not entities really

def trim_name(name, max_len=20): # helper function to create names
    if len(name) <= max_len:
        return name
    return name[:max_len].rsplit(" ", 1)[0]

names = [trim_name(faker.unique.name()) for _ in range(1100)] # creating sets to take data from
STUDENT_NAMES = names[:1000] # maintaining student and instructor pools to prevent overlap
S_NAME_ID = {} # create ID if not yet else use the one prev created since ID won't change
INSTRUCTOR_NAMES = names[1000:]
I_NAME_ID = {}
DEPT_NAMES = ["Arts", "Maths", "Physics", "Biology", "History", "Geography"] # varchar 20
COURSE_TITLES = [faker.sentence(nb_words=3)[:50] for _ in range(100)]
suffixes = ["Hall", "Building", "Center", "Complex", "Tower"]
BUILDINGS = [(faker.street_name().split()[0]+" "+random.choice(suffixes)) for _ in range(20)]

def gen_timeslot():
    ts_id = faker.bothify(text="T###") # bothify gens strs, match pattern, here # replaced with 0-9, eg T320
    day = random.choice(["Monday","Tuesday","Wednesday","Thursday","Friday"])
    start_hour = random.randint(8, 16)
    start_time = f"{start_hour}:00"
    end_time = f"{start_hour}:50"
    record = {
        "time_slot_id": ts_id,
        "day": day,
        "start_time": start_time,
        "end_time": end_time
    }
    TIMESLOT_POOL.append(ts_id) # will need this as many tables refer to this
    return record

def gen_classroom():
    building = random.choice(BUILDINGS)
    room = faker.bothify(text="R###")[:7]
    record = {
        "building": building,
        "room_no": room,
        "capacity": random.randint(20, 200)
    }
    CLASSROOM_POOL.append((building, room))
    return record

def gen_dept(dept = None): # reqs CLASSROOM_POOL
    if not CLASSROOM_POOL:
        return gen_classroom()
    if not dept:
        dept = random.choice(DEPT_NAMES)
    DEPT_POOL.append(dept)
    return {
        "dept_name": dept,
        "building": random.choice(BUILDINGS), # varchar 30
        "budget": round(random.uniform(500000, 5000000), 2) # max digs 12, max left to dec pt 2
    }

def gen_student(dept_name=None): # reqs DEPT_POOL
    if not dept_name:
        if not DEPT_POOL:
            return gen_dept()
        dept_name = random.choice(DEPT_POOL)
    name = random.choice(STUDENT_NAMES)
    if name not in S_NAME_ID:
        S_NAME_ID[name]=faker.bothify(text="S####")
    record = {
        "student_id": S_NAME_ID[name], # varchar 5
        "student_name": name, # varchar 20
        "dept_name": dept_name, # varchar 20
        "tot_cred": random.randint(16, 32) # int
    }
    STUDENT_POOL.append(S_NAME_ID[name])
    return record

def gen_instructor(dept_name=None): # reqs DEPT_POOL
    if not dept_name:
        if not DEPT_POOL:
            return gen_dept()
        dept_name = random.choice(DEPT_POOL)
    name = random.choice(INSTRUCTOR_NAMES) # same logic as student
    if name not in I_NAME_ID.keys():
        I_NAME_ID[name]=faker.bothify(text="I####")
    record = {
        "instructor_id": I_NAME_ID[name], # varchar 5
        "instructor_name": name, # varchar 20
        "dept_name": dept_name,
        "salary": round(random.uniform(100000, 1000000), 2) # max 10 digs total, max 2 after dec pt
    }
    INSTRUCTOR_POOL.append(I_NAME_ID[name])
    return record

def gen_course(dept_name=None): # reqs DEPT_POOL
    if not dept_name:
        if not DEPT_POOL:
            return gen_dept()
        dept_name = random.choice(DEPT_POOL)
    course_id = faker.bothify(text="CS####")
    record = {
        "course_id": course_id,
        "title": random.choice(COURSE_TITLES),
        "dept_name": dept_name,
        "credits": random.randint(1, 5)
    }
    COURSE_POOL.append(course_id)
    return record

def gen_section(): # reqs COURSE_POOL, CLASSROOM_POOL, TIMESLOT_POOL
    if not COURSE_POOL:
        return gen_course()
    if not CLASSROOM_POOL:
        return gen_classroom()
    if not TIMESLOT_POOL:
        return gen_timeslot()
    course = random.choice(COURSE_POOL) # fk
    building, room = random.choice(CLASSROOM_POOL) # fk
    ts_id = random.choice(TIMESLOT_POOL) # fk
    sec_id = faker.bothify(text="SEC##") # varchar 8
    semester = random.choice(["Fall","Spring","Summer","Winter"]) # varchar 6
    year = random.randint(2021, 2025)
    record = {
        "course_id": course,
        "sec_id": sec_id,
        "semester": semester,
        "year": year,
        "building": building,
        "room_no": room,
        "time_slot_id": ts_id
    }
    SECTION_POOL.append((course, sec_id, semester, year))
    return record

def gen_teaches(instructor_id=None): # reqs INSTRUCTOR_POOL, SECTION_POOL
    if instructor_id:
        if not SECTION_POOL:
            return None
    else:
        if not INSTRUCTOR_POOL:
            return gen_instructor()
        instructor_id = random.choice(INSTRUCTOR_POOL)
    if not SECTION_POOL:
        return gen_section()
    course, sec, sem, year = random.choice(SECTION_POOL)
    return {
        "instructor_id": instructor_id,
        "course_id": course,
        "sec_id": sec,
        "semester": sem,
        "year": year
    }

def gen_takes(student_id=None): # reqs STUDENT_POOL, SECTION_POOL
    if student_id:
        if not SECTION_POOL:
            return None
    else:
        if not STUDENT_POOL:
            return gen_student()
        student_id = random.choice(STUDENT_POOL)
    if not SECTION_POOL:
        return gen_section()
    course, sec, sem, year = random.choice(SECTION_POOL)
    return {
        "student_id": student_id,
        "course_id": course,
        "sec_id": sec,
        "semester": sem,
        "year": year,
        "grade": random.choice(["A","B","C","D","F","A-","B+"])
    }

def gen_advisor(student_id=None): # reqs STUDENT_POOL, INSTRUCTOR_POOL
    if student_id:
        if not INSTRUCTOR_POOL:
            return None
    else:
        if not STUDENT_POOL:
            return gen_student()
        student_id = random.choice(STUDENT_POOL)
    if not INSTRUCTOR_POOL:
        return gen_instructor()
    return {
        "student_id": student_id,
        "instructor_id": random.choice(INSTRUCTOR_POOL)
    }

def gen_prereq(course=None): # reqs COURSE_POOL
    if course:
        if len(COURSE_POOL)<2:
            return None
    else:
        if len(COURSE_POOL) < 2: # if less than 2 courses, can't have prereqs
            return gen_course() # some chance of this happening, gettin a rec you didn't ask for
        course = random.choice(COURSE_POOL)
    prereq = [] # generates nested structure
    for _ in range(min(3, len(COURSE_POOL)-1)):
        pr = random.choice(COURSE_POOL)
        while pr == course or pr in prereq: # they shouldn't be the same
            pr = random.choice(COURSE_POOL)
        prereq.append(pr)
    return {
        "course_id": course,
        "prereq_id": prereq
    }

TABLE_GENERATORS = {
    "student": gen_student,
    "instructor": gen_instructor,
    "department": gen_dept,
    "course": gen_course,
    "section": gen_section,
    "classroom": gen_classroom,
    "time_slot": gen_timeslot,
    "takes": gen_takes,
    "teaches": gen_teaches,
    "advisor": gen_advisor,
    "prereq": gen_prereq
}

PAIRABLE_TABLES = [
    ("department", "student"), # dept_name
    ("department", "instructor"), # dept_name
    ("department", "course"), # dept_name
    ("student", "takes"), # student_id
    ("instructor", "teaches"), # instructor_id
    ("course", "prereq"), # course_id
    ("student", "advisor") # student_id
] # gen a rec of tab 1, use common key from both to gen rec for tab 2, combined flat dict

TABLE_WEIGHTS = {
    "department": 0.2,
    "student": 0.8,
    "instructor": 0.7,
    "course": 0.6,
    "classroom": 0.3,
    "time_slot": 0.3,
    "section": 0.2,
    "takes": 0.4,
    "advisor": 0.2,
    "prereq": 0.2,
    "teaches": 0.4
}

USER_POOL = [faker.user_name() for _ in range(20)] # adding username is mandatory

def choose_table(): # to randomise fields being sent for data entry
    tables = list(TABLE_WEIGHTS.keys())
    weights = list(TABLE_WEIGHTS.values())
    return random.choices(tables, weights=weights, k=1)[0]

def find_available_uniques(): # find entities which have already been added
    av_uni = [] # don't want to get rec from []
    if STUDENT_POOL:
        av_uni.append("student_id")
    if INSTRUCTOR_POOL:
        av_uni.append("instructor_id")
    if COURSE_POOL:
        av_uni.append("course_id")
    if TIMESLOT_POOL:
        av_uni.append("time_slot_id")
    if DEPT_POOL:
        av_uni.append("dept_name")
    return av_uni

def generate_add_req():
    if random.random()<0.3: # 30 pc chance of rec spanning multiple tables
        tabs = random.choice(PAIRABLE_TABLES)
        rec = TABLE_GENERATORS[tabs[0]]()
        if tabs[0]=="department":
            common = "dept_name"
        elif tabs[0]=="course":
            common = "course_id"
        elif tabs[0]=="student":
            common = "student_id"
        else:
            common = "instructor_id"
        if common not in rec:
            return generate_add_req()
        rec_ex = TABLE_GENERATORS[tabs[1]](rec[common])
        if rec_ex:
            rec.update(rec_ex)
    else: # 70 pc chance of rec belonging to one table
        table = choose_table()
        generator = TABLE_GENERATORS[table]
        rec = generator()
    rec["username"] = random.choice(USER_POOL) # username is mandatory
    op = {"event":"add", "data":json.dumps(rec)}
    return op

def generate_get_req(): # look at the pools, requirement is to give one unique key, ask for column names
    av_uni = find_available_uniques()
    if not av_uni:
        return generate_add_req()
    uni = random.choice(av_uni)
    rec = {}
    match uni:
        case "student_id":
            rec["student_id"] = random.choice(STUDENT_POOL)
            choice=random.random()
            if choice<=0.3:
                ask_for_cols = ["student_name", "dept_name", "tot_cred"]
            elif choice<=0.6:
                ask_for_cols = ["course_id", "sec_id","semester","year","grade"]
            else:
                ask_for_cols = ["instructor_id"]
        case "instructor_id":
            rec["instructor_id"] = random.choice(INSTRUCTOR_POOL)
            choice=random.random()
            if choice<=0.5:
                ask_for_cols = ["instructor_name", "dept_name", "salary"]
            else:
                ask_for_cols = ["course_id", "sec_id","semester","year"]
        case "time_slot_id":
            rec["time_slot_id"]=random.choice(TIMESLOT_POOL)
            ask_for_cols=["day", "start_time", "end_time"]
        case "course_id":
            rec["course_id"]=random.choice(COURSE_POOL)
            ask_for_cols=["title","dept_name", "credits","prereq_id"]
        case _:
            rec["dept_name"]=random.choice(DEPT_POOL)
            ask_for_cols = ["instructor_name",  "student_name", "course_id"]
    rec["username"] = random.choice(USER_POOL)
    rec["COLUMNS"]=ask_for_cols
    op = {"event":"get", "data":json.dumps(rec)}
    return op

def generate_change_req():
    av_uni = find_available_uniques()
    if not av_uni:
        return generate_add_req()
    uni = random.choice(av_uni)
    rec  = {}
    match uni:
        case "student_id":
            rec["student_id"] = random.choice(STUDENT_POOL)
            rec["tot_cred"] = random.randint(16, 32)
        case "instructor_id":
            rec["instructor_id"] = random.choice(INSTRUCTOR_POOL)
            rec["salary"] = round(random.uniform(100000, 1000000), 2)
        case "time_slot_id":
            ts = random.choice(TIMESLOT_POOL)
            rec["time_slot_id"] = ts
            rec["day"] = random.choice(["Monday","Tuesday","Wednesday","Thursday","Friday"])
        case "course_id":
            rec["course_id"] = random.choice(COURSE_POOL)
            rec["credits"] = random.randint(1, 5)
        case _:
            rec["dept_name"] = random.choice(DEPT_POOL)
            rec["budget"] = round(random.uniform(500000, 5000000),2)
    rec["username"] = random.choice(USER_POOL)
    op = {"event":"change", "data":json.dumps(rec)}
    return op

def generate_remove_req():
    av_uni = find_available_uniques()
    if not av_uni:
        return generate_add_req()
    uni = random.choice(av_uni)
    rec  = {}
    match uni:
        case "student_id":
            value = random.choice(STUDENT_POOL)
            rec["student_id"] = value
            STUDENT_POOL.remove(value)
        case "instructor_id":
            value = random.choice(INSTRUCTOR_POOL)
            rec["instructor_id"] = value
            INSTRUCTOR_POOL.remove(value)
        case "time_slot_id":
            value = random.choice(TIMESLOT_POOL)
            rec["time_slot_id"] = value
            TIMESLOT_POOL.remove(value)
        case "course_id":
            value = random.choice(COURSE_POOL)
            rec["course_id"] = value
            COURSE_POOL.remove(value)
        case _:
            value = random.choice(DEPT_POOL)
            rec["dept_name"] = value
            DEPT_POOL.remove(value)
    rec["username"] = random.choice(USER_POOL)
    op = {"event":"remove", "data":json.dumps(rec)}
    return op

