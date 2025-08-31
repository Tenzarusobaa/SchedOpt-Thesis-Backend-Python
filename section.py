import mysql.connector
import string
from math import ceil

db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '', 
    'database': 'schedopt_db'
}

MAX_PER_SECTION = 40
ALPHABET = string.ascii_uppercase

def section_students():
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("DELETE FROM tbl_program_sections")

    cursor.execute("""
        SELECT fe.fe_program_abbr, fe.fe_year_level, fe.fe_enrolled_count, pd.pd_priority_index
        FROM tbl_forecasted_enrolled fe
        JOIN tbl_program_department pd ON fe.fe_program_abbr = pd.pd_program_abbr
    """)
    rows = cursor.fetchall()

    for row in rows:
        abbr = row['fe_program_abbr']
        year = row['fe_year_level']
        total = row['fe_enrolled_count']
        priority = row['pd_priority_index']

        # Total section number (case of 40 students)
        num_sections = ceil(total / MAX_PER_SECTION)

        # Flexible balancing of schedules
        base = total // num_sections
        remainder = total % num_sections

        for i in range(num_sections):
            group = ALPHABET[i]
            section_final = f"{abbr}-{year}-{group}"
            population = base + 1 if i < remainder else base

            cursor.execute("""
                INSERT INTO tbl_program_sections (
                    ps_program_abbr, ps_year_level, ps_section_group,
                    ps_section_final, ps_section_population, ps_priority_index
                )
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (abbr, year, group, section_final, population, priority))

    conn.commit()
    cursor.close()
    conn.close()
    print("Sectioning completed with capped and balanced student counts.")

if __name__ == "__main__":
    section_students()
