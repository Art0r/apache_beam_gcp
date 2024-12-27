"""
Setup internal packages for project 
"""
from setuptools import find_packages, setup


if __name__ == "__main__":
    setup(
        name="curso_apache_beam_gcp_ext_lib",
        version="1.0",
        install_requires=[
            "psycopg2-binary==2.9.10",
            "google-cloud-secret-manager==2.22.0",
            "cloud-sql-python-connector==1.15.0",
            "SQLAlchemy==2.0.36",
            "apache-beam[gcp]==2.61.0",
            "pg8000==1.31.2",
            "certifi==2024.12.14",
            "python-dotenv==1.0.1",
            "build",
            "setuptools"
        ],
        packages=find_packages(
            include=["curso_apache_beam_gcp", "curso_apache_beam_gcp.*"]),
        zip_safe=False,
        author="Arthur Fernandes",
        author_email="arthur0139@gmail.com",
        description="ETL with Apache Beam example"
    )
