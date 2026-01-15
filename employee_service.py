from db_utils import get_db_connection

class EmployeeService:
    @staticmethod
    def is_marketing_staff(emp_id):
        """Checks if an employee is a Manager in 'Marketing' or a Worker with 'Marketing' position."""
        if not emp_id:
            return False

        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            # We check both Manager department and Worker position
            query = """
                    SELECT e.EmployeeID
                    FROM Employee e
                             LEFT JOIN Manager m ON e.EmployeeID = m.EmployeeID
                             LEFT JOIN Worker w ON e.EmployeeID = w.EmployeeID
                    WHERE e.EmployeeID = %s
                      AND (m.Department = 'Marketing' OR w.Position = 'Marketing')
                    """
            cursor.execute(query, (emp_id,))
            result = cursor.fetchone()
            return result is not None
        finally:
            cursor.close()
            conn.close()

            

    @staticmethod
    def is_manager(emp_id):
        """Checks if the employee is a Manager (has entry in Manager table)."""
        if not emp_id:
            return False

        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            query = """
                SELECT e.EmployeeID
                FROM Employee e
                JOIN Manager m ON e.EmployeeID = m.EmployeeID
                WHERE e.EmployeeID = %s
            """
            cursor.execute(query, (emp_id,))
            result = cursor.fetchone()
            return result is not None
        finally:
            cursor.close()
            conn.close()