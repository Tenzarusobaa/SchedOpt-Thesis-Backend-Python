import mysql.connector
from mysql.connector import Error
from collections import defaultdict
from datetime import datetime, time
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class CourseScheduler:
    def __init__(self, db_config):
        self.db_config = db_config
        self.connection = None
        self.cursor = None
        self.assigned_sections = set()
        self.room_assignments = defaultdict(set) 
        self.section_assignments = set()
        self.program_section_assignments = defaultdict(list)
        self.program_section_time_blocks = defaultdict(list)  # Track consecutive time blocks
        self.min_break_minutes = 80  # require at least one full 80-min slot as a break

        
    def connect(self):
        """Establish database connection"""
        try:
            self.connection = mysql.connector.connect(**self.db_config)
            self.cursor = self.connection.cursor(dictionary=True)
            print("Database connection established")
        except Error as e:
            print(f"Error connecting to MySQL: {e}")
    
    def disconnect(self):
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            self.cursor.close()
            self.connection.close()
            print("Database connection closed")
    
    def clear_existing_assignments(self):
        """Clear existing assignments from final_assignment table"""
        try:
            self.cursor.execute("TRUNCATE TABLE tbl_final_assignment")
            self.cursor.execute("TRUNCATE TABLE tbl_initial_assignments")
            self.connection.commit()
            print("Cleared existing assignments")
        except Error as e:
            print(f"Error clearing assignments: {e}")
    
    def query_course_sections(self):
        """Retrieve all course sections that need scheduling"""
        try:
            query = """
                SELECT cs_course_section, cs_program_section, cs_student_count, 
                    cs_department, cs_course_type, cs_units, cs_course_year
                FROM tbl_course_section
                ORDER BY 
                    CASE 
                        WHEN cs_course_type = 'PATHFIT' THEN 1  -- Schedule PATHFIT first
                        WHEN cs_department IN ('SLA', 'SMA', 'SED') AND (cs_course_type = 'MSC' OR cs_course_type = 'ELEC' OR cs_course_type = 'MISC') THEN 2
                        WHEN cs_department = 'CSITE' THEN 3  -- Schedule CSITE after SMA/SLA/SED
                        ELSE 4
                    END,
                    cs_student_count DESC
            """
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Error as e:
            print(f"Error fetching course sections: {e}")
            return []
    
    def query_available_rooms(self, course_type, student_count, department=None, units=None, program_section=None):
        """Retrieve available rooms based on course type requirements with program-specific prioritization"""
        try:
            # Base query for room data
            room_query = """
                SELECT rd_room_code, rd_type, rd_function, rd_capacity,
                    rd_department_owner, rd_program_owner, rd_size
                FROM tbl_room_data
                WHERE rd_capacity >= %s
            """
        
            # Group SMA, SLA, and SED departments together
            if department in ['SLA', 'SMA', 'SED'] and (course_type in ['MSC', 'ELEC', 'MISC', 'CMP', 'CAE', 'PEC']):
                room_query += " AND rd_type = 'LEC' AND rd_function = 'LEC'"
                if units == 3:
                    duration = 80
                    day_type = 'Pair'
                elif units == 6:
                    duration = 170
                    day_type = 'Pair'
                else:
                    return []
            
            # Handle CSITE department courses - PROGRAM-SPECIFIC ROOMS FIRST
            elif department == 'CSITE' and (course_type in ['MSC', 'MISC', 'ELEC', 'CMP', 'CAE', 'PEC']):
                # BSCS programs - prioritize ADVANCED labs but allow BASIC as fallback
                if (program_section and 'BSCS' in program_section):
                    room_query += " AND (rd_function IN ('ADVANCED', 'RESEARCH', 'BASIC'))"
                    if units == 3:
                        duration = 80
                        day_type = 'Pair'
                    elif units == 6:
                        duration = 170
                        day_type = 'Pair'
                    else:
                        return []
                
                # BSIT programs - prioritize BASIC labs but allow ADVANCED as fallback
                elif (program_section and 'BSIT' in program_section):
                    room_query += " AND (rd_function IN ('BASIC', 'ADVANCED') OR rd_type = 'LEC')"
                    if units == 3:
                        duration = 80
                        day_type = 'Pair'
                    elif units == 6:
                        duration = 170
                        day_type = 'Pair'
                    else:
                        return []
                
                # BSNMCA program - prioritize ANIMATION rooms but allow LEC as fallback
                elif (program_section and 'BSNMCA' in program_section):
                    room_query += " AND (rd_function IN ('ANIMATION', 'LEC') OR rd_type = 'LEC')"
                    if units == 3:
                        duration = 80
                        day_type = 'Pair'
                    elif units == 6:
                        duration = 170
                        day_type = 'Pair'
                    else:
                        return []
                
                # BSMATH program - prioritize MATH-specific rooms but allow LEC as fallback
                elif (program_section and 'BSMATH' in program_section):
                    room_query += " AND ((rd_type = 'LAB' AND rd_program_owner = 'BSMATH') OR rd_type = 'LEC')"
                    if units == 3:
                        duration = 80
                        day_type = 'Pair'
                    elif units == 6:
                        duration = 170
                        day_type = 'Pair'
                    else:
                        return []
                    
                # BSECE program - prioritize ELECTRONICS and ENGINEERING labs but allow LEC as fallback
                elif (program_section and 'BSECE' in program_section):
                    room_query += " AND ((rd_type = 'LAB' AND rd_function IN ('ELECTRONICS', 'ENGINEERING')) OR rd_type = 'LEC')"
                    if units == 3:
                        duration = 80
                        day_type = 'Pair'
                    elif units == 6:
                        duration = 170
                        day_type = 'Pair'
                    else:
                        return []
                    
                # BSCPE program - prioritize ELECTRONICS and ENGINEERING labs but allow LEC as fallback
                elif (program_section and 'BSCPE' in program_section):
                    room_query += " AND ((rd_type = 'LAB' AND rd_function IN ('ADVANCED', 'ENGINEERING')) OR rd_type = 'LEC')"
                    if units == 3:
                        duration = 80
                        day_type = 'Pair'
                    elif units == 6:
                        duration = 170
                        day_type = 'Pair'
                    else:
                        return []
                #BSCE
                elif (program_section and 'BSCE' in program_section):
                    room_query += " AND rd_type = 'LEC' AND rd_function = 'LEC'"
                    if units == 3:
                        duration = 80
                        day_type = 'Pair'
                    elif units == 6:
                        duration = 170
                        day_type = 'Pair'
                    else:
                        return []
                #BSBME
                elif (program_section and 'BSBME' in program_section):
                    room_query += " AND rd_type = 'LEC' AND rd_function = 'LEC'"
                    if units == 3:
                        duration = 80
                        day_type = 'Pair'
                    elif units == 6:
                        duration = 170
                        day_type = 'Pair'
                    else:
                        return []
                
                else:
                    return []
            
            elif course_type in ['NGEC', 'GEELECT', 'NSTP', 'CC']:
                room_query += " AND rd_type = 'LEC' AND rd_function = 'LEC'"
                duration = 80
                day_type = 'Pair'
            
            elif course_type == 'PATHFIT':
                room_query += " AND rd_type = 'GYM' AND rd_function = 'PATHFIT' AND rd_room_code LIKE 'MPCC%'"
                duration = 120
                day_type = 'Single'
            else:
                return []
            
            self.cursor.execute(room_query, (student_count,))
            all_rooms = self.cursor.fetchall()
            
            # Determine preferred room size based on student count (for sorting only)
            if student_count <= 10:
                size_priority = ['S', 'M', 'L']  # Small rooms first
            elif student_count <= 25:
                size_priority = ['M', 'S', 'L']  # Medium rooms first
            else:
                size_priority = ['L', 'M', 'S']  # Large rooms first
            
            # Sort rooms: program-specific rooms first, then by size priority
            program_specific_rooms = []
            other_rooms = []
            
            for room in all_rooms:
                # Check if room is program-specific
                is_program_specific = False
                
                if program_section:
                    # BSNMCA should prioritize ANIMATION rooms
                    if 'BSNMCA' in program_section and room['rd_function'] == 'ANIMATION':
                        is_program_specific = True
                    # BSCS should prioritize ADVANCED labs but also allow BASIC as program-specific
                    elif 'BSCS' in program_section and (room['rd_function'] == 'ADVANCED' or room['rd_function'] == 'BASIC'):
                        is_program_specific = True
                    # BSIT should prioritize BASIC labs but also allow ADVANCED as program-specific
                    elif 'BSIT' in program_section and (room['rd_function'] == 'BASIC' or room['rd_function'] == 'ADVANCED'):
                        is_program_specific = True
                    # BSMATH should prioritize MATH-specific labs
                    elif 'BSMATH' in program_section and (room['rd_function'] == 'LAB' and room['rd_program_owner'] == 'BSMATH'):
                        is_program_specific = True
                    # BSECE should prioritize ELECTRONICS or ENGINEERING labs
                    elif 'BSECE' in program_section and (room['rd_function'] == 'ELECTRONICS' or room['rd_function'] == 'ENGINEERING'):
                        is_program_specific = True
                    # BSCPE should prioritize ELECTRONICS or ENGINEERING labs
                    elif 'BSCPE' in program_section and (room['rd_function'] == 'ADVANCED' or room['rd_function'] == 'ENGINEERING'):
                        is_program_specific = True
                
                if is_program_specific:
                    program_specific_rooms.append(room)
                else:
                    other_rooms.append(room)
            
            # Sort program-specific rooms by size priority
            sorted_program_specific = []
            for size in size_priority:
                size_rooms = [room for room in program_specific_rooms if room['rd_size'] == size]
                sorted_program_specific.extend(size_rooms)
            
            # Sort other rooms by size priority
            sorted_other_rooms = []
            for size in size_priority:
                size_rooms = [room for room in other_rooms if room['rd_size'] == size]
                sorted_other_rooms.extend(size_rooms)
            
            # Combine: program-specific rooms first, then others
            sorted_rooms = sorted_program_specific + sorted_other_rooms
            
            # Get all time slots that match our requirements
            time_query = """
                SELECT ts_start_time, ts_end_time, ts_duration 
                FROM tbl_time_slot 
                WHERE ts_duration = %s
            """
            self.cursor.execute(time_query, (duration,))
            time_slots = self.cursor.fetchall()
            
            # Get all days that match our requirements
            day_query = "SELECT ds_abbr FROM tbl_day_slot WHERE ds_day_type = %s"
            self.cursor.execute(day_query, (day_type,))
            days = [row['ds_abbr'] for row in self.cursor.fetchall()]
            
            # Generate all possible combinations
            available_slots = []
            for room in sorted_rooms:
                for day in days:
                    for time_slot in time_slots:
                        available_slots.append({
                            'rdta_room_code': room['rd_room_code'],
                            'rdta_day_abbr': day,
                            'rdta_start_time': time_slot['ts_start_time'],
                            'rdta_end_time': time_slot['ts_end_time'],
                            'rdta_room_capacity': room['rd_capacity'],
                            'rdta_day_type': day_type,
                            'rdta_room_type': room['rd_type'],
                            'rdta_room_function': room['rd_function'],
                            'rdta_ts_duration': time_slot['ts_duration'],
                            'rdta_room_size': room['rd_size'],
                            'rdta_is_program_specific': room in program_specific_rooms  # Flag for debugging
                        })
            
            return available_slots
        except Error as e:
            print(f"Error fetching available rooms: {e}")
            return []
    
    def time_to_minutes(self, time_str):
        """Convert time string (e.g., '8:00 AM') to minutes since midnight"""
        time_obj = datetime.strptime(time_str, '%I:%M %p').time()
        return time_obj.hour * 60 + time_obj.minute
    
    def parse_day_abbr(self, day_abbr):
        """Parse day abbreviation to return list of individual days"""
        day_mapping = {
            'M': ['Monday'],
            'T': ['Tuesday'],
            'W': ['Wednesday'],
            'Th': ['Thursday'],
            'F': ['Friday'],
            'S': ['Saturday'],
            'MTh': ['Monday', 'Thursday'],
            'TF': ['Tuesday', 'Friday'],
            'WS': ['Wednesday', 'Saturday']
        }
        return day_mapping.get(day_abbr, [])
    
    def has_time_overlap(self, program_section, day_abbr, new_start, new_end):
        """Check if the new time overlaps with any existing assignment for this program section"""
        new_start_min = self.time_to_minutes(new_start)
        new_end_min = self.time_to_minutes(new_end)
        
        # Parse the new day abbreviation to get actual days
        new_days = self.parse_day_abbr(day_abbr)
        
        for existing_day_abbr, existing_start, existing_end in self.program_section_assignments[program_section]:
            # Parse the existing day abbreviation to get actual days
            existing_days = self.parse_day_abbr(existing_day_abbr)
            
            # Check if any day overlaps between the two sets
            overlapping_days = set(new_days) & set(existing_days)
            if not overlapping_days:
                continue  # Different days, no overlap
            
            existing_start_min = self.time_to_minutes(existing_start)
            existing_end_min = self.time_to_minutes(existing_end)
            
            # Check for time overlap on overlapping days
            if not (new_end_min <= existing_start_min or new_start_min >= existing_end_min):
                return True
                
        return False
    
    def violates_consecutive_limit(self, program_section, day_abbr, new_start, new_end):
        """Check if adding this class would exceed the 170-minute consecutive limit"""
        new_start_min = self.time_to_minutes(new_start)
        new_end_min = self.time_to_minutes(new_end)

        for day in self.parse_day_abbr(day_abbr):
            # pull the exact-day blocks (stored with full day names)
            time_blocks = self.program_section_time_blocks.get((program_section, day), [])

            # start with the new block's minutes
            chain_minutes = new_end_min - new_start_min

            for block_start, block_end in time_blocks:
                overlaps = not (new_end_min <= block_start or new_start_min >= block_end)
                gap_before = new_start_min - block_end   # existing ends then new starts
                gap_after = block_start - new_end_min    # new ends then existing starts

                # treat as consecutive if overlapping OR the gap is smaller than the required break
                if overlaps or (0 <= gap_before < self.min_break_minutes) or (0 <= gap_after < self.min_break_minutes):
                    chain_minutes += (block_end - block_start)

            if chain_minutes > 170:
                return True

        return False
    
    def is_assignment_valid(self, room_code, day_abbr, start_time, end_time, 
                        course_section, program_sections, room_size, student_count, is_program_specific):
        """Check if the assignment doesn't conflict with existing assignments"""

        new_start_min = self.time_to_minutes(start_time)
        new_end_min = self.time_to_minutes(end_time)

        # ✅ Check room conflicts with time overlap
        for (existing_room, existing_day, existing_start, existing_end), assigned_courses in self.room_assignments.items():
            if existing_room == room_code and existing_day == day_abbr:
                ex_start_min = self.time_to_minutes(existing_start)
                ex_end_min = self.time_to_minutes(existing_end)

                # If times overlap, reject
                if not (new_end_min <= ex_start_min or new_start_min >= ex_end_min):
                    return False

        # Check if this course section is already scheduled
        if course_section in self.section_assignments:
            return False
        
        # Check if any program section has a time overlap
        for program_section in program_sections.split(', '):
            program_section = program_section.strip()
            if not program_section:
                continue
                
            if self.has_time_overlap(program_section, day_abbr, start_time, end_time):
                return False
                
            if self.violates_consecutive_limit(program_section, day_abbr, start_time, end_time):
                return False
        
        return True

    
    def update_time_blocks(self, program_section, day_abbr, start_time, end_time):
        """Update the time blocks for a program section to track consecutive classes"""
        start_min = self.time_to_minutes(start_time)
        end_min = self.time_to_minutes(end_time)

        for day in self.parse_day_abbr(day_abbr):
            key = (program_section, day)
            time_blocks = self.program_section_time_blocks.get(key, [])

            new_block = (start_min, end_min)
            merged_blocks = []

            for block_start, block_end in time_blocks:
                overlaps = not (end_min <= block_start or start_min >= block_end)
                gap_before = start_min - block_end
                gap_after = block_start - end_min

                if overlaps or (0 <= gap_before < self.min_break_minutes) or (0 <= gap_after < self.min_break_minutes):
                    # merge into a longer consecutive block
                    new_block = (min(new_block[0], block_start), max(new_block[1], block_end))
                else:
                    merged_blocks.append((block_start, block_end))

            merged_blocks.append(new_block)
            self.program_section_time_blocks[key] = merged_blocks
    
    def record_initial_assignment(self, course_section, room_code, day_abbr, start_time, end_time):
        """Record the assignment in tbl_initial_assignments"""
        try:
            insert_query = """
                INSERT INTO tbl_initial_assignments 
                (ia_course_section, ia_room_code, ia_day_abbr, ia_start_time, ia_end_time)
                VALUES (%s, %s, %s, %s, %s)
            """
            values = (course_section, room_code, day_abbr, start_time, end_time)
            self.cursor.execute(insert_query, values)
            self.connection.commit()
            return True
        except Error as e:
            print(f"Error recording initial assignment: {e}")
            self.connection.rollback()
            return False
    
    def assign_section(self, section, room):
        """Assign a section to a room-day-time slot"""
        try:
            # Get program sections as a list
            program_sections = section['cs_program_section']
            
            # Check if assignment is valid
            if not self.is_assignment_valid(
                room['rdta_room_code'], 
                room['rdta_day_abbr'], 
                room['rdta_start_time'], 
                room['rdta_end_time'], 
                section['cs_course_section'],
                program_sections,
                room['rdta_room_size'],  # Add room size
                section['cs_student_count'],  # Add student count
                room.get('rdta_is_program_specific', False)  # Add program-specific flag
            ):
                return False
            
            # Record the initial assignment
            if not self.record_initial_assignment(
                section['cs_course_section'],
                room['rdta_room_code'],
                room['rdta_day_abbr'],
                room['rdta_start_time'],
                room['rdta_end_time']
            ):
                return False
            
            # Create the final timeslot string by concatenating start and end times
            final_timeslot = f"{room['rdta_start_time']} - {room['rdta_end_time']}"
            
            # Insert the final assignment with the concatenated timeslot
            insert_query = """
                INSERT INTO tbl_final_assignment 
                (fa_course_section, fa_program_section, fa_student_count, 
                 fa_department, fa_room_code, fa_day_abbr, fa_start_time, fa_end_time, fa_course_year, fa_final_timeslot)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                section['cs_course_section'],
                program_sections,
                section['cs_student_count'],
                section['cs_department'],
                room['rdta_room_code'],
                room['rdta_day_abbr'],
                room['rdta_start_time'],
                room['rdta_end_time'],
                section['cs_course_year'],
                final_timeslot  # Add the concatenated timeslot
            )
            
            self.cursor.execute(insert_query, values)
            self.connection.commit()
            
            # Update tracking sets
            self.room_assignments[(room['rdta_room_code'], room['rdta_day_abbr'], room['rdta_start_time'], room['rdta_end_time'])].add(section['cs_course_section'])
            self.section_assignments.add(section['cs_course_section'])
            
            # Track program sections and their time slots
            for program_section in program_sections.split(', '):
                program_section = program_section.strip()
                if program_section:
                    self.program_section_assignments[program_section].append((
                        room['rdta_day_abbr'],
                        room['rdta_start_time'],
                        room['rdta_end_time']
                    ))
                    
                    # Update time blocks to track consecutive classes
                    self.update_time_blocks(
                        program_section,
                        room['rdta_day_abbr'],
                        room['rdta_start_time'],
                        room['rdta_end_time']
                    )
            
            room_type = "PROGRAM-SPECIFIC" if room.get('rdta_is_program_specific') else "GENERAL"
            print(f"Assigned {section['cs_course_section']} ({section['cs_course_type']}) to {room['rdta_room_code']} ({room_type}) on {room['rdta_day_abbr']} at {final_timeslot}")
            return True
        except Error as e:
            print(f"Error assigning section: {e}")
            self.connection.rollback()
            return False
    
    def schedule_courses(self):
        """Main scheduling function"""
        self.connect()
        self.clear_existing_assignments()
        
        # Get all course sections that need scheduling
        course_sections = self.query_course_sections()
        
        if not course_sections:
            print("No course sections found to schedule")
            return
        
        # Schedule each course section
        for section in course_sections:
            # Get available rooms for this section type
            available_rooms = self.query_available_rooms(
                section['cs_course_type'], 
                section['cs_student_count'],
                section.get('cs_department'),
                section.get('cs_units'),
                section.get('cs_program_section')
            )
            
            if not available_rooms:
                print(f"No available rooms found for {section['cs_course_section']} ({section['cs_course_type']})")
                continue
            
            # Debug: Show available rooms
            print(f"Available rooms for {section['cs_course_section']} ({section['cs_student_count']} students, {section['cs_program_section']}):")
            for i, room in enumerate(available_rooms[:8]):  # Show first 8 options
                room_type = "PROGRAM-SPECIFIC" if room.get('rdta_is_program_specific') else "GENERAL"
                print(f"  {i+1}. {room['rdta_room_code']} (size: {room['rdta_room_size']}, cap: {room['rdta_room_capacity']}, type: {room_type})")
            
            # Try to assign to available rooms in order
            assigned = False
            for room in available_rooms:
                if self.assign_section(section, room):
                    assigned = True
                    break
            
            if not assigned:
                print(f"Failed to assign {section['cs_course_section']} ({section['cs_course_type']}) - no valid slots available")
        
        self.disconnect()
        print("Scheduling completed")

# Database configuration
db_config = {
    'host': os.getenv("MYSQLHOST", "localhost"),
    'user': os.getenv("MYSQLUSER", "root"),
    'password': os.getenv("MYSQLPASSWORD", ""),
    'database': os.getenv("MYSQLDATABASE", "schedopt_db"),
    'port': int(os.getenv("MYSQLPORT", 3306))
}

# Run the scheduler
scheduler = CourseScheduler(db_config)
scheduler.schedule_courses()