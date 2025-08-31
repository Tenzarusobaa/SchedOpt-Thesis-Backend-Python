import mysql.connector
from collections import defaultdict
import sys
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


SEMESTER = 1
if len(sys.argv) > 1:
    try:
        SEMESTER = int(sys.argv[1])
    except ValueError:
        SEMESTER = 1


def query_db_connection():
    """Establish database connection (works locally and in Railway)"""
    return mysql.connector.connect(
        host=os.getenv("MYSQLHOST", "localhost"),
        user=os.getenv("MYSQLUSER", "root"),
        password=os.getenv("MYSQLPASSWORD", ""),
        database=os.getenv("MYSQLDATABASE", "schedopt_db"),
        port=int(os.getenv("MYSQLPORT", 3306))
    )
def query_program_sections():
    """Retrieve all program sections with their details"""
    conn = query_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    query = """
    SELECT ps.ps_program_abbr, ps.ps_year_level, ps.ps_section_group, 
           ps.ps_section_final, ps.ps_section_population, ps.ps_priority_index,
           pd.pd_department
    FROM tbl_program_sections ps
    JOIN tbl_program_department pd ON ps.ps_program_abbr = pd.pd_program_abbr
    ORDER BY ps.ps_priority_index, pd.pd_department, ps.ps_program_abbr, 
             ps.ps_year_level, ps.ps_section_group
    """
    cursor.execute(query)
    sections = cursor.fetchall()
    
    cursor.close()
    conn.close()
    return sections

def query_prospectus_courses():
    """Retrieve all prospectus courses for the selected semester"""
    conn = query_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    query = """
    SELECT pl_program, pl_department, pl_year, pl_course_code, 
           pl_course_title, pl_units, pl_semester, pl_type
    FROM tbl_prospectus_list
    WHERE pl_semester = %s
    """
    cursor.execute(query, (SEMESTER,))
    courses = cursor.fetchall()
    
    cursor.close()
    conn.close()
    return courses

def create_course_sections():
    """Main function to create course sections"""
    
    program_sections = query_program_sections()
    prospectus_courses = query_prospectus_courses()
    
    print(f"Processing courses for semester {SEMESTER}...")
    print(f"Found {len(prospectus_courses)} courses in this semester.")
    
    # Dictionary to track used course section names and their next available letter
    course_section_tracker = defaultdict(lambda: {'next_letter': 'A'})
    
    # Group courses by course code and year (regardless of department)
    course_groups = defaultdict(list)
    for course in prospectus_courses:
        key = (course['pl_course_code'], course['pl_year'])
        course_groups[key].append(course)
    
    # Group program sections by department and year
    program_groups = defaultdict(list)
    for section in program_sections:
        key = (section['pd_department'], section['ps_year_level'])
        program_groups[key].append(section)
    
    # Prepare course section data
    course_sections = []
    
    # Process each course group (same course code and year, across all departments)
    for (course_code, year), courses in course_groups.items():
        # Reset section letter for each course code + year combination
        section_letter = 'A'
        
        # Process each department that has this course
        departments_with_course = {course['pl_department'] for course in courses}
        
        for department in departments_with_course:
            # Get all program sections in this department and year
            sections_in_dept_year = program_groups.get((department, year), [])
            
            # Find which program sections have this course in their prospectus
            sections_with_course = []
            for section in sections_in_dept_year:
                # Check if this section's program has this course in its prospectus
                for course in courses:
                    if (course['pl_program'] == section['ps_program_abbr'] and 
                        course['pl_department'] == department):
                        sections_with_course.append(section)
                        break
            
            if not sections_with_course:
                continue
            
            # Get course details (assuming all courses with same code/year/dept have same type/semester/units)
            course_details = next((c for c in courses if c['pl_department'] == department), None)
            if not course_details:
                continue
                
            course_type = course_details['pl_type']
            units = course_details['pl_units']
            
            # Sort sections by priority index and program
            sections_with_course.sort(key=lambda x: (x['ps_priority_index'], x['ps_program_abbr']))
            
            # Group sections into course sections (max 40 students)
            current_group = []
            current_count = 0
            
            for section in sections_with_course:
                if current_count + section['ps_section_population'] <= 40:
                    current_group.append(section)
                    current_count += section['ps_section_population']
                else:
                    # Create a course section for the current group
                    if current_group:
                        course_section_name = f"{course_code}-{year}-{section_letter}"
                        create_course_section_record(
                            course_section_name, 
                            current_group, current_count, department, 
                            course_sections, course_type, SEMESTER, units, year
                        )
                        section_letter = chr(ord(section_letter) + 1)  # Next letter
                    
                    # Start new group with current section
                    current_group = [section]
                    current_count = section['ps_section_population']
            
            # Add the last group
            if current_group:
                course_section_name = f"{course_code}-{year}-{section_letter}"
                create_course_section_record(
                    course_section_name,
                    current_group, current_count, department, 
                    course_sections, course_type, SEMESTER, units, year
                )
                section_letter = chr(ord(section_letter) + 1)  # Next letter
    
    # Insert into database
    insert_course_sections(course_sections)

def create_course_section_record(course_section_name, sections, student_count, department, course_sections, course_type, semester, units, year):
    """Create a course section record"""
    # Format program sections (comma-separated list)
    program_sections = ", ".join(s['ps_section_final'] for s in sections)
    
    course_sections.append({
        'cs_course_section': course_section_name,
        'cs_program_section': program_sections,
        'cs_student_count': student_count,
        'cs_department': department,
        'cs_course_type': course_type,
        'cs_semester': semester,
        'cs_units': units,
        'cs_course_year': year  # Added course year
    })

def insert_course_sections(course_sections):
    """Insert course sections into database"""
    if not course_sections:
        print("No course sections to insert.")
        return
    
    conn = query_db_connection()
    cursor = conn.cursor()
    
    # Clear existing data (optional)
    cursor.execute("DELETE FROM tbl_course_section")
    
    # Prepare insert statement (updated to include cs_course_year)
    insert_query = """
    INSERT INTO tbl_course_section 
    (cs_course_section, cs_program_section, cs_student_count, cs_department, 
     cs_course_type, cs_semester, cs_units, cs_course_year)
    VALUES (%(cs_course_section)s, %(cs_program_section)s, 
            %(cs_student_count)s, %(cs_department)s, %(cs_course_type)s,
            %(cs_semester)s, %(cs_units)s, %(cs_course_year)s)
    """
    
    # Insert all records
    cursor.executemany(insert_query, course_sections)
    conn.commit()
    
    print(f"Inserted {len(course_sections)} course sections for semester {SEMESTER}.")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    # Clear the table before starting
    conn = query_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM tbl_course_section")
    cursor.close()
    conn.close()
    
    create_course_sections()