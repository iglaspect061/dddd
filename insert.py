from data_access import insert_student as _insert


def insert_student(student_id: int, name: str, age: int, email: str, phone: str, courses: str) -> None:
    _insert(student_id, name, age, email, phone, courses)

