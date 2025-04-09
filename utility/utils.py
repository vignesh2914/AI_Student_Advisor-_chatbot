def get_student_info(db, student_name):
    # Step 1: Get EMPLID
    query1 = f"""
    SELECT EMPLID, ACAD_PROG, CUM_GPA 
    FROM student_enrollment 
    WHERE NAME_DISPLAY LIKE '%{student_name}%';
    """
    student_result = db.run(query1)
    if not student_result or len(student_result) == 0:
        return None
    
    emplid = student_result[0]['EMPLID']
    acad_prog = student_result[0]['ACAD_PROG']
    gpa = student_result[0]['CUM_GPA']
    
    # Step 2: Get strong subjects
    query2 = f"""
    SELECT SUBJECT, COURSE_TITLE_LONG, CRSE_GRADE_OFF 
    FROM student_enrollment 
    WHERE EMPLID = '{emplid}' 
    ORDER BY CRSE_GRADE_OFF DESC;
    """
    subjects = db.run(query2)

    return {
        "EMPLID": emplid,
        "ACAD_PROG": acad_prog,
        "CUM_GPA": gpa,
        "SUBJECTS": subjects
    }

def get_elective_recommendations(db, acad_prog, strong_subject):
    query = f"""
    SELECT DISTINCT Subject, Catalog, Descr, Mtg_Start, Mtg_End, Mon, Tue, Wed, Thurs, Fri 
    FROM class_schedule 
    WHERE Subject = '{strong_subject}' AND Term = '2025A' AND Campus IS NOT NULL
    LIMIT 3;
    """
    return db.run(query)
