from __future__ import annotations

from dataclasses import dataclass

from db import execute, fetch_all, fetch_one


def _normalize_courses(courses: str) -> list[str]:
    parts = [c.strip() for c in (courses or "").split(",")]
    dedup: list[str] = []
    seen = set()
    for p in parts:
        if not p:
            continue
        key = p.casefold()
        if key in seen:
            continue
        seen.add(key)
        dedup.append(p)
    return dedup


def ensure_course(name: str) -> int:
    # MySQL: no "RETURNING", so we do insert-ignore then select
    execute("INSERT IGNORE INTO Course (CourseName) VALUES (%s)", (name,))
    row = fetch_one("SELECT CourseID FROM Course WHERE CourseName = %s", (name,))
    if not row:
        raise RuntimeError("Failed to create/find course.")
    return int(row["CourseID"])


def set_student_courses(student_id: int, courses_csv: str) -> None:
    courses = _normalize_courses(courses_csv)
    # replace mapping
    execute("DELETE FROM StudentCourse WHERE StudentID = %s", (student_id,))
    for c in courses:
        cid = ensure_course(c)
        execute(
            "INSERT IGNORE INTO StudentCourse (StudentID, CourseID) VALUES (%s, %s)",
            (student_id, cid),
        )
    # keep legacy CourseS field in sync (optional)
    execute(
        "UPDATE StudentDetails SET CourseS = %s WHERE StudentID = %s",
        (", ".join(courses), student_id),
    )


def upsert_student_details(student_id: int, phone: str, courses_csv: str) -> None:
    # Upsert phone + legacy courses string
    execute(
        """
        INSERT INTO StudentDetails (StudentID, PhoneNumber, CourseS)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
          PhoneNumber = VALUES(PhoneNumber),
          CourseS = VALUES(CourseS)
        """,
        (student_id, phone, courses_csv),
    )
    set_student_courses(student_id, courses_csv)


def list_students(student_id: int | None = None) -> list[dict]:
    where = ""
    params: tuple = ()
    if student_id is not None:
        where = "WHERE s.StudentID = %s"
        params = (student_id,)
    return fetch_all(
        f"""
        SELECT
          s.StudentID,
          s.Name,
          s.Age,
          s.Email,
          d.PhoneNumber,
          COALESCE(
            NULLIF(GROUP_CONCAT(DISTINCT c.CourseName ORDER BY c.CourseName SEPARATOR ', '), ''),
            d.CourseS
          ) AS Courses
        FROM Student s
        LEFT JOIN StudentDetails d ON d.StudentID = s.StudentID
        LEFT JOIN StudentCourse sc ON sc.StudentID = s.StudentID
        LEFT JOIN Course c ON c.CourseID = sc.CourseID
        {where}
        GROUP BY s.StudentID, s.Name, s.Age, s.Email, d.PhoneNumber, d.CourseS
        ORDER BY s.StudentID DESC
        """,
        params,
    )


def get_student(student_id: int) -> dict | None:
    rows = list_students(student_id)
    return rows[0] if rows else None


def insert_student(student_id: int, name: str, age: int, email: str, phone: str, courses_csv: str) -> None:
    execute(
        "INSERT INTO Student (StudentID, Name, Age, Email) VALUES (%s, %s, %s, %s)",
        (student_id, name, age, email),
    )
    upsert_student_details(student_id, phone, courses_csv)


def update_student(student_id: int, name: str, age: int, email: str) -> None:
    execute(
        "UPDATE Student SET Name = %s, Age = %s, Email = %s WHERE StudentID = %s",
        (name, age, email, student_id),
    )


def delete_student(student_id: int) -> None:
    execute("DELETE FROM Student WHERE StudentID = %s", (student_id,))


def list_courses() -> list[dict]:
    return fetch_all(
        """
        SELECT
          c.CourseID,
          c.CourseName,
          COUNT(sc.StudentID) AS StudentCount
        FROM Course c
        LEFT JOIN StudentCourse sc ON sc.CourseID = c.CourseID
        GROUP BY c.CourseID, c.CourseName
        ORDER BY c.CourseName
        """
    )


def create_course(course_name: str) -> None:
    ensure_course(course_name.strip())


def rename_course(course_id: int, new_name: str) -> None:
    execute("UPDATE Course SET CourseName = %s WHERE CourseID = %s", (new_name.strip(), course_id))


def delete_course(course_id: int) -> None:
    execute("DELETE FROM Course WHERE CourseID = %s", (course_id,))

